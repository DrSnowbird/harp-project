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

package edu.iu.harp.collective;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.WorkerInfo;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.Workers.WorkerInfoList;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.Receiver;
import edu.iu.harp.depl.CMDOutput;
import edu.iu.harp.depl.QuickDeployment;

public class BenchmarkDriver {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(BenchmarkDriver.class);
  private static final String bcast_worker_script = QuickDeployment
    .getProjectHomePath()
    + QuickDeployment.getBinDirectory()
    + "collective/start_chainbcast_worker.sh";
  private static final String regroup_worker_script = QuickDeployment
    .getProjectHomePath()
    + QuickDeployment.getBinDirectory()
    + "collective/start_regroup_worker.sh";
  private static final String wordcount_worker_script = QuickDeployment
    .getProjectHomePath()
    + QuickDeployment.getBinDirectory()
    + "collective/start_wordcount_worker.sh";
  private static final String allgather_worker_script = QuickDeployment
    .getProjectHomePath()
    + QuickDeployment.getBinDirectory()
    + "collective/start_allgather_worker.sh";
  private static final String graph_worker_script = QuickDeployment
    .getProjectHomePath()
    + QuickDeployment.getBinDirectory()
    + "collective/start_graph_worker.sh";

  public static void main(String args[]) throws Exception {
    // Get driver host and port
    String driverHost = args[0];
    int driverPort = Integer.parseInt(args[1]);
    String task = args[2];
    // Initialize
    LOG.info("Initialize driver (worker scheduler).");
    long jobID = 0;
    WorkerData workerData = new WorkerData();
    ResourcePool resourcePool = new ResourcePool();
    Workers workers = new Workers();
    Receiver receiver = new Receiver(workerData, resourcePool, workers,
      driverHost, driverPort, Constants.NUM_HANDLER_THREADS);
    receiver.start();
    // Start workers
    LOG.info("Start all workers...");
    boolean isRunning = false;
    if (task.equals("bcast")) {
      // args[3]: totalByteData
      // args[4]: numLoops
      isRunning = startAllWorkers(workers, workerData, resourcePool,
        bcast_worker_script, driverHost, driverPort, jobID, args[3], args[4]);
    } else if (task.equals("regroup")) {
      // args[3]: partitionByteData
      // args[4]: numPartitions
      isRunning = startAllWorkers(workers, workerData, resourcePool,
        regroup_worker_script, driverHost, driverPort, jobID, args[3], args[4]);
    } else if (task.equals("wordcount")) {
      isRunning = startAllWorkers(workers, workerData, resourcePool,
        wordcount_worker_script, driverHost, driverPort, jobID);
    } else if (task.equals("allgather")) {
      isRunning = startAllWorkers(workers, workerData, resourcePool,
        allgather_worker_script, driverHost, driverPort, jobID, args[3],
        args[4]);
    } else if (task.equals("graph")) {
   // args[3]: iteration
      isRunning = startAllWorkers(workers, workerData, resourcePool,
        graph_worker_script, driverHost, driverPort, jobID, args[3]);
    } else {
      LOG.info("Inccorect task command... ");
    }
    if (isRunning) {
      waitForAllWorkers(workers, workerData, resourcePool);
    }
    receiver.stop();
    System.exit(0);
  }

  private static boolean startAllWorkers(Workers workers,
    WorkerData workerData, ResourcePool resourcePool, String script,
    String driverHost, int driverPort, long jobID, String... otherArgs) {
    boolean success = true;
    String workerNode;
    int workerID;
    WorkerInfoList workerInfoList = workers.getWorkerInfoList();
    for (WorkerInfo workerInfo : workerInfoList) {
      workerNode = workerInfo.getNode();
      workerID = workerInfo.getID();
      success = startWorker(workerNode, script, driverHost, driverPort,
        workerID, jobID, otherArgs);
      LOG.info("Start worker ID: " + workerInfo.getID() + " Node: "
        + workerInfo.getNode() + " Result: " + success);
      if (!success) {
        return false;
      }
    }
    return true;
  }

  private static boolean startWorker(String workerHost, String script,
    String driverHost, int driverPort, int workerID, long jobID,
    String... otherArgs) {
    int otherArgsLen = otherArgs.length;
    String cmdstr[] = new String[8 + otherArgsLen];
    cmdstr[0] = "ssh";
    cmdstr[1] = workerHost;
    cmdstr[2] = script;
    cmdstr[3] = driverHost;
    cmdstr[4] = driverPort + "";
    cmdstr[5] = workerID + "";
    cmdstr[6] = jobID + "";
    for (int i = 0; i < otherArgsLen; i++) {
      cmdstr[7 + i] = otherArgs[i];
    }
    cmdstr[cmdstr.length - 1] = "&";

    CMDOutput cmdOutput = QuickDeployment.executeCMDandNoWait(cmdstr);
    return cmdOutput.getExecutionStatus();
  }

  private static boolean waitForAllWorkers(Workers workers,
    WorkerData workerData, ResourcePool resourcePool) {
    byte[] workerACKs = new byte[workers.getNumWorkers()];
    int count = 0;
    while (count < workers.getNumWorkers()) {
      WorkerStatus status = (WorkerStatus) workerData
        .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (status != null) {
        LOG.info("Worker Status: " + status.getWorkerID());
        workerACKs[status.getWorkerID()] = 1;
        count++;
        resourcePool.getWritableObjectPool().releaseWritableObjectInUse(status);
      }
    }
    return true;
  }
}
