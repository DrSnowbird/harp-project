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

package edu.iu.harp.depl;

import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

class Setup {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(Setup.class);

  // Worker nodes host names
  private Nodes nodes;

  // private boolean isOnNFS;

  Setup(Nodes nodes, boolean onNFS) {
    this.nodes = nodes;
    // this.isOnNFS = onNFS;
  }

  boolean configure() {
    boolean status = false;
    LOG.info("Setup nodes file...");
    status = setupNodesFile();
    if (!status) {
      LOG.error("Errors when writing to nodes file.");
      return false;
    }
    LOG.info("Setup harp.properties...");
    status = setupProperties();
    if (!status) {
      LOG.error("Errors in writing to harp.properties file.");
      return false;
    }
    return status;
  }

  /**
   * write back node IPs to nodes
   * 
   * @return
   */
  private boolean setupNodesFile() {
    this.nodes.sortRacks();
    return QuickDeployment.writeToFile(QuickDeployment.nodes_file,
      this.nodes.printToNodesFile());
  }

  /**
   * Set ".properties" file.
   */
  private boolean setupProperties() {
    boolean status = true;
    Properties properties = new Properties();
    try {
      String home = QuickDeployment.prjt_home;
      FileReader reader = new FileReader(QuickDeployment.harp_properties);
      properties.load(reader);
      reader.close();
      // Clear original properties.
      properties.clear();
      // Set daemons port base
      properties.setProperty(QuickDeployment.key_worker_port_base, "12500");
      LOG.info(QuickDeployment.key_worker_port_base + "="
        + properties.getProperty(QuickDeployment.key_worker_port_base));
      // Set compute threads per worker, we set it to a smaller value than all
      // the cores available
      int numThreads = (int) Math.ceil(Runtime.getRuntime()
        .availableProcessors() * (double) 2 / (double) 3);
      properties.setProperty(QuickDeployment.key_compute_threads_per_worker,
        numThreads + "");
      LOG.info(QuickDeployment.key_compute_threads_per_worker
        + "="
        + properties
          .getProperty(QuickDeployment.key_compute_threads_per_worker));
      // Set app dir
      String app_dir_value = home + "apps";
      properties.setProperty(QuickDeployment.key_app_dir, app_dir_value);
      LOG
        .info("app_dir=" + properties.getProperty(QuickDeployment.key_app_dir));
      // Set data_dir
      String dir = createDataDir();
      if (dir == null) {
        dir = "";
        status = false;
      }
      properties.setProperty(QuickDeployment.key_data_dir, dir);
      LOG.info("data_dir="
        + properties.getProperty(QuickDeployment.key_data_dir));
      // Read comments and write
      String comments = QuickDeployment
        .readPropertyComments(QuickDeployment.harp_properties);
      if (comments == null) {
        comments = "";
        status = false;
      }
      FileWriter writer = new FileWriter(QuickDeployment.harp_properties);
      properties.store(writer, comments);
      writer.close();
    } catch (Exception e) {
      status = false;
      e.printStackTrace();
    }
    return status;
  }

  private String createDataDir() {
    LOG.info("Create common data dir, please wait...");
    boolean status = true;
    // First level dir
    String data_dir1 = null;
    String username = QuickDeployment.getUserName();
    if (username == null) {
      return null;
    }
    data_dir1 = "/tmp/" + username + "/";
    LOG.info("First level dir: " + data_dir1);
    status = QuickDeployment.createDir(data_dir1);
    if (status) {
      LOG.info("Directory path " + data_dir1 + " are created on all nodes.");
    } else {
      return null;
    }
    // Second level dir
    String data_dir2 = data_dir1 + "data/";
    LOG.info("Second level dir: " + data_dir2);
    status = QuickDeployment.createDir(data_dir2);
    if (status) {
      LOG.info("Directory path " + data_dir2 + " are created on all nodes.");
    } else {
      return null;
    }
    return data_dir2;
  }

  /**
   * Package all the things under project Home, and then send to remote folder
   * and tarx them.
   */
  @SuppressWarnings("unused")
  private boolean distribute() {
    boolean status = false;
    // Get dir name and package name
    String[] result = QuickDeployment.getUpperDirAndCurrent(QuickDeployment
      .getProjectHomePath());
    String srcDir = result[0];
    String packageName = result[1];
    LOG.info("Project package replication: srcDir: " + srcDir
      + " packageName: " + packageName);
    // Assume srcDir is equal to destDir and it exists there
    String destDir = srcDir;
    // First, build tar package
    status = QuickDeployment.buildProjectTar(srcDir, packageName, destDir);
    if (!status) {
      return false;
    }
    // Enter home folder and then execute tar copy and untar
    List<String> nodeList = this.nodes.getNodeList();
    for (int i = 0; i < nodeList.size(); i++) {
      status = QuickDeployment.scpAndTarx(destDir + "/" + packageName
        + ".tar.gz", nodeList.get(i), destDir.toString());
      if (!status) {
        return false;
      }
    }
    return true;
  }
}
