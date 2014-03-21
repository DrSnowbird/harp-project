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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * Select suitable setup mode, and execute setup works of ActiveMQ only
 * 
 * @author zhangbj
 * 
 */
public class QuickDeployment {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(QuickDeployment.class);

  final static String java_home = getJavaHome();
  final static String prjt_home = getProjectHome();
  final static String bin_directory = "scripts/";
  final static String nodes_file = prjt_home + bin_directory + "nodes";
  final static String stimr_sh = prjt_home + bin_directory + "stimr.sh";
  final static String tar_java_Sh = prjt_home + bin_directory
    + "build_java_distribution.sh";
  final static String tar_prjt_sh = prjt_home + bin_directory
    + "build_harp_distribution.sh";
  final static String copy_tarx_sh = prjt_home + bin_directory
    + "scp_tarx_file.sh";
  final static String dfs_sh = prjt_home + bin_directory + "dfs.sh";

  public final static String harp_properties = prjt_home + bin_directory
    + "harp.properties";
  public final static String key_worker_port_base = "worker_port";
  public final static String worker_port_base = "12500";
  public final static String key_compute_threads_per_worker = "compute_threads_per_worker";
  public final static String key_app_dir = "app_dir";
  public final static String key_data_dir = "data_dir";

  // retrieve output of the command execution
  private static CMDOutput executeCMDandReturn(String[] cmd) {
    CMDOutput cmdOutput = new CMDOutput();
    List<String> output = cmdOutput.getExecutionOutput();
    try {
      Process q = Runtime.getRuntime().exec(cmd);
      q.waitFor();
      InputStream is = q.getInputStream();
      InputStreamReader isr = new InputStreamReader(is);
      BufferedReader br = new BufferedReader(isr);

      String line;
      while ((line = br.readLine()) != null) {
        output.add(line);
      }
      br.close();
      if (q.exitValue() != 0) {
        cmdOutput.setExecutionStatus(false);
        output.clear();
      }
    } catch (Exception e) {
      LOG.error("Errors happen in executing " + cmd);
      cmdOutput.setExecutionStatus(false);
      output.clear();
    }
    return cmdOutput;
  }

  /**
   * Just do execution. The output is not returned, it shows on the screen
   * directly
   * 
   * @param cmd
   */
  public static CMDOutput executeCMDandForward(String[] cmd) {
    CMDOutput cmdOutput = new CMDOutput();
    try {
      Process q = Runtime.getRuntime().exec(cmd);
      q.waitFor();
      if (q.exitValue() != 0) {
        cmdOutput.setExecutionStatus(false);
      }
    } catch (Exception e) {
      LOG.error("Errors in executing " + cmd);
      cmdOutput.setExecutionStatus(false);
    }
    return cmdOutput;
  }

  public static CMDOutput executeCMDandNoWait(String[] cmd) {
    CMDOutput cmdOutput = new CMDOutput();
    try {
      Runtime.getRuntime().exec(cmd);
      cmdOutput.setExecutionStatus(true);
    } catch (Exception e) {
      LOG.error("Errors in executing " + cmd, e);
      cmdOutput.setExecutionStatus(false);
    }
    return cmdOutput;
  }

  // probably we need a executeCMD without return command
  static String getPWD() {
    String cmdstr[] = { "pwd" };
    CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
    String pwd = null;
    if (cmdOutput.getExecutionStatus()) {
      pwd = cmdOutput.getExecutionOutput().get(0).replace(" ", "\\ ");
    }
    return pwd;
  }

  static String getUserHome() {
    String cmdstr[] = { "bash", "-c", "echo $HOME" };
    CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
    String home = null;
    if (cmdOutput.getExecutionStatus()) {
      home = cmdOutput.getExecutionOutput().get(0).replace(" ", "\\ ");
    }
    return home;
  }

  private static String getJavaHome() {
    // It seems the only way to execute echo command
    String cmdstr[] = { "bash", "-c", "echo $JAVA_HOME" };
    CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
    String java_home = null;
    if (cmdOutput.getExecutionStatus()) {
      // Home directory is returned with "/" at the end
      java_home = cmdOutput.getExecutionOutput().get(0) + "/";
      java_home = java_home.replace("//", "/");
      java_home = java_home.replace(" ", "\\ ");
    }
    return java_home;
  }

  private static String getProjectHome() {
    // It seems the only way to execute echo command
    String cmdstr[] = { "bash", "-c", "echo $HARP_HOME" };
    CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
    String harp_home = null;
    if (cmdOutput.getExecutionStatus()) {
      // Home directory is returned with "/" at the end
      harp_home = cmdOutput.getExecutionOutput().get(0) + "/";
      harp_home = harp_home.replace("//", "/");
      harp_home = harp_home.replace(" ", "\\ ");
    }
    return harp_home;
  }

  public static String getJavaHomePath() {
    return java_home;
  }

  public static String getProjectHomePath() {
    return prjt_home;
  }

  public static String getBinDirectory() {
    return bin_directory;
  }

  static int getTotalMem() {
    String mem = "0";
    String cmdstr[] = { "cat", "/proc/meminfo" };
    CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
    if (cmdOutput.getExecutionStatus()) {
      for (int i = 0; i < cmdOutput.getExecutionOutput().size(); i++) {
        if (cmdOutput.getExecutionOutput().get(i).contains("MemTotal:")) {
          mem = cmdOutput.getExecutionOutput().get(i).replace("MemTotal:", "")
            .replace("kB", "").trim();
          break;
        }
      }
    }
    double memory = Double.parseDouble(mem);
    int memMB = (int) (memory / (double) 1024);
    return memMB;
  }

  static boolean createDir(String path) {
    File dfsShFile = new File(dfs_sh);
    dfsShFile.setExecutable(true);
    String cmdstr[] = { dfs_sh, "initdir", path };
    CMDOutput cmdOutput = QuickDeployment.executeCMDandForward(cmdstr);
    return cmdOutput.getExecutionStatus();
  }

  static String getUserName() {
    String cmdstr[] = { "whoami" };
    CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
    String userName = null;
    if (cmdOutput.getExecutionStatus()) {
      userName = cmdOutput.getExecutionOutput().get(0);
    }
    return userName;
  }

  public static boolean scpFile(String file, String dest) {
    String cmdstr[] = { "scp", file, dest };
    CMDOutput cmdOutput = QuickDeployment.executeCMDandForward(cmdstr);
    return cmdOutput.getExecutionStatus();
  }

  /**
   * build project tar package
   * 
   * @param srcDir
   * @param packageName
   * @param destDir
   * @return
   */
  static boolean buildProjectTar(String srcDir, String packageName,
    String destDir) {
    return QuickDeployment.buildTar(tar_prjt_sh, srcDir, packageName, destDir);
  }

  /**
   * Build Java tar package
   * 
   * @param srcDir
   * @param packageName
   * @param destDir
   * @return
   */
  static boolean buildJavaTar(String srcDir, String packageName, String destDir) {
    return QuickDeployment.buildTar(tar_java_Sh, srcDir, packageName, destDir);
  }

  static boolean buildTar(String tarSh, String srcDir, String packageName,
    String destDir) {
    File tarShFile = new File(tarSh);
    tarShFile.setExecutable(true);
    String cmdstr[] = { tarSh, srcDir, packageName, destDir };
    CMDOutput cmdOutput = QuickDeployment.executeCMDandForward(cmdstr);
    return cmdOutput.getExecutionStatus();
  }

  /**
   * Copy tar package to remote and extract
   * 
   * @param file
   * @param destIP
   * @param destDir
   * @return
   */
  static boolean scpAndTarx(String filePath, String destIP, String destDir) {
    File copyAndTarxShFile = new File(copy_tarx_sh);
    copyAndTarxShFile.setExecutable(true);
    String cmdstr[] = { copy_tarx_sh, filePath, destIP, destDir };
    CMDOutput cmdOutput = QuickDeployment.executeCMDandForward(cmdstr);
    return cmdOutput.getExecutionStatus();
  }

  /**
   * Only write one line to the file
   * 
   * @param filePath
   * @param oneLine
   */
  static boolean writeToFile(String filePath, String oneLine) {
    List<String> contents = new ArrayList<String>();
    contents.add(oneLine);
    return writeToFile(filePath, contents);
  }

  /**
   * Write a list of strings to lines
   * 
   * @param filePath
   * @param contents
   */
  static boolean writeToFile(String filePath, List<String> contents) {
    boolean status = true;
    // delete the original file
    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
      for (int i = 0; i < contents.size(); i++) {
        writer.write(contents.get(i));
        // if it is not the last line, enter
        if (i < (contents.size() - 1)) {
          writer.newLine();
        }
      }
      writer.flush();
      writer.close();
    } catch (Exception e) {
      // e.printStackTrace();
      System.err.println("Errors happen in writing " + filePath);
      status = false;
    }
    return status;
  }

  /**
   * if output is null, we think there is error...
   * 
   * @param properties_filename
   * @return
   */
  static String readPropertyComments(String properties_filename) {
    StringBuffer comments = new StringBuffer();
    String commentsString = null;
    try {
      BufferedReader reader = new BufferedReader(new FileReader(
        properties_filename));
      String line = null;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("#")) {
          comments.append(line.replace("#", "") + "\n");
        }
      }
      reader.close();
    } catch (Exception e) {
      LOG.error("Errors happen in reading " + properties_filename);
      comments = null;
    }
    if (comments != null) {
      commentsString = new String(comments);
    }
    return commentsString;
  }

  /**
   * Test if the IP is the current local IP
   * 
   * @param ip
   * @return
   */
  static boolean isLocalIP(String ip) {
    boolean status = false;
    try {
      Enumeration<NetworkInterface> netInterfaces = NetworkInterface
        .getNetworkInterfaces();
      for (NetworkInterface netInterface : Collections.list(netInterfaces)) {
        Enumeration<InetAddress> inetAddresses = netInterface
          .getInetAddresses();
        for (InetAddress inetAddress : Collections.list(inetAddresses)) {
          if (ip.equals(inetAddress.getHostAddress())) {
            status = true;
            break;
          }
        }
      }
    } catch (SocketException e) {
      LOG.error("Errors when checking if IP is local...");
    }
    return status;
  }

  /**
   * Be careful! For non-existing path, it will report as local.
   * 
   * @param path
   * @return
   */
  public static boolean isLocalDir(String path) {
    boolean status = true;
    String cmdstr[] = { "bash", "-c", "stat -f -L -c %T " + path };
    CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
    if (cmdOutput.getExecutionStatus()) {
      if (cmdOutput.getExecutionOutput().get(0).equals("nfs")) {
        status = false;
      }
    }
    return status;
  }

  @SuppressWarnings("unused")
  private static boolean checkJavaLocation() {
    boolean isJavaOnNFS = false;
    if (!isLocalDir(QuickDeployment.getJavaHomePath())) {
      isJavaOnNFS = true;
    }
    return isJavaOnNFS;
  }

  @SuppressWarnings("unused")
  private static boolean checkProjectLocation() {
    boolean isOnNFS = false;
    if (!isLocalDir(QuickDeployment.getProjectHomePath())) {
      isOnNFS = true;
    }
    return isOnNFS;
  }

  /**
   * Is this line a tag of rack?
   * 
   * @param line
   * @return
   */
  static boolean isRack(String line) {
    Pattern p = Pattern.compile("#[0-9]*");
    Matcher m = p.matcher(line);
    return m.matches();
  }

  static int getRackID(String line) {
    return Integer.parseInt(line.substring(1));
  }

  public static String[] getUpperDirAndCurrent(String path) {
    // Get dir name and package name
    StringBuffer pathBuffer = new StringBuffer(path);
    if (pathBuffer.lastIndexOf("/") == pathBuffer.length() - 1) {
      pathBuffer.deleteCharAt(pathBuffer.length() - 1);
    }
    int slashPos = pathBuffer.lastIndexOf("/");
    String upperDir = pathBuffer.substring(0, slashPos);
    String current = pathBuffer.substring(slashPos + 1);
    String[] results = { upperDir, current };
    return results;
  }

  /**
   * Similar to project package distribution, build, copy and untar
   */
  @SuppressWarnings("unused")
  private static boolean distributeJava(Nodes nodes) {
    boolean status = false;
    // Get dir name and package name
    String[] result = getUpperDirAndCurrent(QuickDeployment.getJavaHomePath());
    String srcDir = result[0];
    String packageName = result[1];
    LOG.info("Java package replication: srcDir: " + srcDir + " packageName: "
      + packageName);
    // Assume srcDir is equal to destDir and it exists there
    String destDir = srcDir;
    // First, build tar package
    status = QuickDeployment.buildJavaTar(srcDir, packageName, destDir);
    if (!status) {
      return false;
    }
    // Enter home folder and then execute tar copy and untar
    List<String> nodeList = nodes.getNodeList();
    for (int i = 0; i < nodeList.size(); i++) {
      status = QuickDeployment.scpAndTarx(destDir + "/" + packageName
        + ".tar.gz", nodeList.get(i), destDir.toString());
      if (!status) {
        return false;
      }
    }
    return true;
  }

  public static void main(String[] args) {
    boolean invalidJavaHome = false;
    boolean invalidProjectHome = false;
    boolean invalidNodes = false;
    boolean isProjectOnNFS = false;
    boolean configurationStatus = true;
    // Java home detection
    // Assume that java is installed on every node.
    // Here only pick the current node to check.
    if (QuickDeployment.getJavaHomePath() == null) {
      invalidJavaHome = true;
    } else {
      File javaHomeFile = new File(QuickDeployment.getJavaHomePath());
      if (!javaHomeFile.exists()) {
        invalidJavaHome = true;
      }
    }
    if (invalidJavaHome) {
      LOG.info("Java Home is not set properly...");
      System.exit(-1);
    } else {
      LOG.info("Java Home: " + QuickDeployment.getJavaHomePath());
    }
    // Project home detection
    if (QuickDeployment.getProjectHomePath() == null) {
      invalidProjectHome = true;
    } else {
      File homeFile = new File(QuickDeployment.getProjectHomePath());
      if (!homeFile.exists()) {
        invalidProjectHome = true;
      }
    }
    if (invalidProjectHome) {
      LOG.info("Harp Home is not set properly...");
      System.exit(-1);
    } else {
      LOG.info("Harp Home: " + QuickDeployment.getProjectHomePath());
    }
    // Check nodes information
    Nodes nodes = null;
    try {
      nodes = new Nodes();
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (nodes == null || nodes.getNumPhysicalNodes() == 0) {
      invalidNodes = true;
    }
    if (invalidNodes) {
      LOG.error("No nodes file or wrong nodes file format is provided.");
      System.exit(-1);
    }
    // Check Project location.
    // The code of checking could be wrong, always
    // set to true on HPC and supercomputer
    isProjectOnNFS = true;
    // Start deployment
    // Start to deploy nodes
    LOG.info("Start deploying nodes.");
    Setup nodesSetup = new Setup(nodes, isProjectOnNFS);
    configurationStatus = nodesSetup.configure();
    if (!configurationStatus) {
      LOG.error(" Errors happen in configuring nodes.");
      System.exit(-1);
    }
    LOG.info("Quick deployment is done.");
  }
}