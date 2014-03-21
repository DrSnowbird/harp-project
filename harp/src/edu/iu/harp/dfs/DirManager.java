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

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import edu.iu.harp.config.Configuration;
import edu.iu.harp.depl.CMDOutput;
import edu.iu.harp.depl.Nodes;
import edu.iu.harp.depl.QuickDeployment;

public class DirManager {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(DirManager.class);

  public static void main(String[] args) throws Exception {   
    // init, mkdir, rmdir
    String action = args[0];
    String dir = args[1];
    Nodes nodes = new Nodes();
    List<String> hosts = nodes.getNodeList();
    Configuration config = new Configuration();
 
    if(action.equals("initdir")) {
      // For initialization of dir, we do it sequentially
      // And no matter if it is local or nfs, we do it on all nodes.
      // And no checking of the reults
      for (String host : hosts) {
        String[] cmd = { "ssh", host, "mkdir", dir };
        LOG.info(cmd[0] + " " + cmd[1] + " " + cmd[2] + " " + cmd[3]);
        QuickDeployment.executeCMDandForward(cmd);
      }
    } else if(action.equals("mkdir") || action.equals("rmdir")) {
      // Check if base dir is local
      boolean isLocalDir = QuickDeployment.isLocalDir(config.getLocalDataDir());
      dir = (config.getLocalDataDir() + "/" + dir).replace("//", "/");
      isLocalDir = QuickDeployment.isLocalDir(dir);
      if(!isLocalDir) {
        String[] cmd = {action, dir};
        CMDOutput output = QuickDeployment.executeCMDandForward(cmd);
        if(!output.getExecutionStatus()) {
          LOG.error("Error when " + action);
        } 
      } else {
        final BlockingQueue<String> hostnameQueue = new ArrayBlockingQueue<String>(
          hosts.size());
        for (String host : hosts) {
          hostnameQueue.add(host);
        }
        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService taskExecutor = Executors.newFixedThreadPool(numThreads);
        AtomicBoolean error = new AtomicBoolean(false);
        for (int i = 0; i < numThreads; i++) {
          taskExecutor.execute(new DirCmdThread(hostnameQueue, 
            action, dir, error));
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
        if (error.get()) {
          LOG.error("Error when " + action);
          System.exit(-1);
        }
      }
    } else {
     LOG.error("Incorrect dir command..."); 
     System.exit(-1);
    }
    System.exit(0);
  }
}
