/*
 * Copyright 2014 Indiana University
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.iu.harp.dfs;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.naming.ConfigurationException;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.log4j.Logger;

import edu.iu.harp.config.Configuration;
import edu.iu.harp.depl.Nodes;
import edu.iu.harp.depl.QuickDeployment;

public class FileDistributor {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(FileDistributor.class);

  private static List<File> getListOfFiles(String directory, String filePattern) {
    File dir = new File(directory);
    FileFilter fileFilter = new WildcardFileFilter(filePattern);
    File[] files = dir.listFiles(fileFilter);
    List<File> selectedFiles = new ArrayList<File>();
    for (File file : files) {
      selectedFiles.add(file);
    }
    return selectedFiles;
  }

  private static List<String> getListOfNodes() throws Exception {
    Nodes nodes = new Nodes();
    return nodes.getNodeList();
  }

  /**
   * We may do hash based mapping in future...
   * 
   * @param files
   * @param hosts
   * @return
   */
  private static Object2ObjectOpenHashMap<String, ObjectArrayList<File>> groupMasterFilesToHosts(
    List<File> files, List<String> hosts) {
    int numOfHosts = hosts.size();
    int div = files.size() / numOfHosts;
    int mod = files.size() % numOfHosts;
    Object2ObjectOpenHashMap<String, ObjectArrayList<File>> groups = new Object2ObjectOpenHashMap<String, ObjectArrayList<File>>();
    ObjectArrayList<File> group = null;
    int start = 0;
    int end = 0;
    // Calculate files per host
    for (int i = 0; i < numOfHosts; i++) {
      end += div;
      if (mod > 0) {
        end++;
        mod--;
      }
      group = new ObjectArrayList<File>();
      for (int j = start; j < end; j++) {
        group.add(files.get(j));
      }
      groups.put(hosts.get(i), group);
      start = end;
    }
    return groups;
  }

  /**
   * Create mapping of replications
   * 
   * @param masterGroups
   * @param numReplications
   * @param isLocalDir
   * @return
   */
  private static Object2ObjectOpenHashMap<String, ObjectArrayList<File>> groupReplicaFilesToHosts(
    Object2ObjectOpenHashMap<String, ObjectArrayList<File>> masterGroups,
    List<String> hosts, int numReplications, boolean isLocalDir) {
    int numOfHosts = hosts.size();
    Object2ObjectOpenHashMap<String, ObjectArrayList<File>> replicaGroups = new Object2ObjectOpenHashMap<String, ObjectArrayList<File>>();
    ObjectArrayList<File> masterGroup = null;
    ObjectArrayList<File> replicaGroup = null;
    // Add replicas if dir is local
    if (numReplications > 1 && isLocalDir) {
      int replicaHostPos = 0;
      for (int i = 0; i < numOfHosts; i++) {
        masterGroup = masterGroups.get(hosts.get(i));
        // Replica includes the original copy
        for (int j = 1; j < numReplications; j++) {
          replicaHostPos = (i + j) % numOfHosts;
          replicaGroup = replicaGroups.get(hosts.get(replicaHostPos));
          if (replicaGroup == null) {
            replicaGroup = new ObjectArrayList<File>();
            replicaGroups.put(hosts.get(replicaHostPos), replicaGroup);
          }
          replicaGroup.addAll(masterGroup);
        }
      }
    } else if (!isLocalDir) {
      for (int i = 0; i < numOfHosts; i++) {
        masterGroup = masterGroups.get(hosts.get(i));
        // Replica includes the original copy
        for (int j = 0; j < numOfHosts; j++) {
          if (j != i) {
            replicaGroup = replicaGroups.get(hosts.get(j));
            if (replicaGroup == null) {
              replicaGroup = new ObjectArrayList<File>();
              replicaGroups.put(hosts.get(j), replicaGroup);
            }
            replicaGroup.addAll(masterGroup);
          }
        }
      }
    }
    return replicaGroups;
  }

  private static void printPartitionFile(String partitionFile,
    String destDirPath,
    Object2ObjectOpenHashMap<String, ObjectArrayList<File>> masterGroups,
    Object2ObjectOpenHashMap<String, ObjectArrayList<File>> replicaGroups)
    throws Exception {
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(partitionFile));
      for (Entry<String, ObjectArrayList<File>> entry : masterGroups.entrySet()) {
        for (File file : entry.getValue()) {
          // hostname,destDirPath,master/replica,fileName,master/replica
          bw.write(entry.getKey() + "," + destDirPath + "," + file.getName()
            + ",m\n");
        }
      }
      for (Entry<String, ObjectArrayList<File>> entry : replicaGroups
        .entrySet()) {
        for (File file : entry.getValue()) {
          // hostname,destDirPath,fileName,master/replica
          bw.write(entry.getKey() + "," + destDirPath + "," + file.getName()
            + ",r\n");
        }
      }
      bw.flush();
      bw.close();
    } catch (Exception e) {
      throw new Exception(e);
    }
  }

  /**
   * Copy and generate partition file
   * 
   * @param args
   * @throws ConfigurationException
   * @throws IOException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws Exception {
    double startTime = System.currentTimeMillis();
    if (args.length < 5 || args.length > 6) {
      LOG.info("Usage:[src directory(local)][destination directory (remote)]"
        + "[file filter][partition file][num duplicates (optional)]");
      return;
    }
    String srcDirPath = args[0];
    String destSubDir = args[1];
    String fileFilter = args[2];
    String partitionFile = args[3];
    int numReplications = 1;
    if (args.length == 5) {
      numReplications = Integer.parseInt(args[4]);
    }
    // Get src dir
    File srcDir = new File(srcDirPath);
    if (!srcDir.exists()) {
      LOG.error("Invalid input directory. Directory does not exist.");
      System.exit(-1);
    }
    // Get dest dir
    Configuration config = new Configuration();
    String destDirPath = (config.getLocalDataDir() + "/" + destSubDir).replace(
      "//", "/");
    LOG.info("Destintion Directory =" + destDirPath);
    // Get files
    List<File> files = getListOfFiles(srcDirPath, fileFilter);
    for (File file : files) {
      LOG.info(file.getAbsolutePath());
    }
    if (files.size() == 0) {
      LOG.error("No files to copy.");
      System.exit(-1);
    }
    LOG.info("Number of files to copy = " + files.size());
    // Get nodes
    List<String> hosts = null;
    try {
      hosts = getListOfNodes();
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (hosts == null || hosts.size() == 0) {
      LOG.error("No nodes specified in nodes file.");
      System.exit(-1);
    }
    LOG.info("Number of nodes = " + hosts.size());

    boolean isLocalDir = QuickDeployment.isLocalDir(destDirPath);
    // Grouping
    Object2ObjectOpenHashMap<String, ObjectArrayList<File>> masterGroups = groupMasterFilesToHosts(
      files, hosts);
    Object2ObjectOpenHashMap<String, ObjectArrayList<File>> replicaGroups = groupReplicaFilesToHosts(
      masterGroups, hosts, numReplications, isLocalDir);

    // Create queue
    final BlockingQueue<String> hostnameQueue = new ArrayBlockingQueue<String>(
      hosts.size());
    for (String host : hosts) {
      hostnameQueue.add(host);
    }
    int numThreads = Runtime.getRuntime().availableProcessors();
    ExecutorService taskExecutor = Executors.newFixedThreadPool(numThreads);
    AtomicBoolean error = new AtomicBoolean(false);
    for (int i = 0; i < numThreads; i++) {
      taskExecutor.execute(new FileDistributeThread(hostnameQueue,
        masterGroups, replicaGroups, destDirPath, error));
    }
    // Shutdown
    taskExecutor.shutdown();
    try {
      // Wait a while for existing tasks to terminate
      if (!taskExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        taskExecutor.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!taskExecutor.awaitTermination(60, TimeUnit.SECONDS))
          LOG.error("Task executor did not terminate");
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      taskExecutor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
    double endTime = System.currentTimeMillis();
    System.out.println("Total Data Copy Time = " + (endTime - startTime) / 1000
      + " Seconds.");

    if (error.get()) {
      System.exit(-1);
    }
    // Print partition file
    printPartitionFile(partitionFile, destDirPath,
      masterGroups, replicaGroups);
    System.exit(0);
  }
}
