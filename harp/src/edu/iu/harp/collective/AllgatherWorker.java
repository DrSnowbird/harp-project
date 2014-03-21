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

import edu.iu.harp.arrpar.ArrCombiner;
import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.allgather.ArrTableCatcher;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.request.AllgatherReq;
import edu.iu.harp.comm.request.ParGenAck;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.Receiver;

public class AllgatherWorker extends CollCommWorker {

  private static final Logger LOG = Logger.getLogger(AllgatherWorker.class);

  public static void main(String args[]) throws Exception {
    String driverHost = args[0];
    int driverPort = Integer.parseInt(args[1]);
    int workerID = Integer.parseInt(args[2]);
    long jobID = Long.parseLong(args[3]);
    int partitionByteSize = Integer.parseInt(args[4]);
    int numPartitions = Integer.parseInt(args[5]);
    initLogger(workerID);
    LOG.info("args[] " + driverHost + " " + driverPort + " " + workerID + " "
      + jobID + " " + partitionByteSize + " " + numPartitions);
    // --------------------------------------------------------------------
    // Worker initialize
    Workers workers = new Workers(workerID);
    String host = workers.getSelfInfo().getNode();
    int port = workers.getSelfInfo().getPort();
    WorkerData workerData = new WorkerData();
    ResourcePool resourcePool = new ResourcePool();
    Receiver receiver = new Receiver(workerData, resourcePool, workers, host,
      port, Constants.NUM_HANDLER_THREADS);
    receiver.start();
    // Master check if all slaves are ready
    boolean success = masterBarrier(workers, workerData, resourcePool);
    LOG.info("Barrier: " + success);
    // ------------------------------------------------------------------------
    // Generate data partition
    ArrTable<DoubleArray, DoubleArrPlus> table = new ArrTable<DoubleArray, DoubleArrPlus>(
      workerID, DoubleArray.class, DoubleArrPlus.class);
    int doublesSize = partitionByteSize / 8;
    if (doublesSize < 3) {
      doublesSize = 3;
    }
    // Generate partition data
    for (int i = 0; i < numPartitions; i++) {
      double[] doubles = new double[doublesSize];
      doubles[0] = 1; // One row
      doubles[1] = 1; // One count
      for (int j = 2; j < doublesSize; j++) {
        doubles[j] = workerID;
      }
      DoubleArray doubleArray = new DoubleArray();
      doubleArray.setArray(doubles);
      doubleArray.setSize(doublesSize);
      ArrPartition<DoubleArray> partition = new ArrPartition<DoubleArray>(
        doubleArray, workerID * numPartitions + i);
      LOG.info("Data Generate, WorkerID: " + workerID + " Partition: "
        + partition.getPartitionID() + " Row count: " + doubles[0]
        + " Size per Column: " + doubles[0] + " First element: " + doubles[2]
        + " Last element: " + doubles[doublesSize - 1]);
      table.addPartition(partition);
    }
    // ------------------------------------------------------------------------
    // Allgather
    allgather(workers, workerData, resourcePool, table);
    for (ArrPartition<DoubleArray> partition : table.getPartitions()) {
      double[] doubles = partition.getArray().getArray();
      int size = partition.getArray().getSize();
      LOG.info(" Partition: " + partition.getPartitionID() + " Row count: "
        + doubles[0] + " Size per Column: " + doubles[0] + " First element: "
        + doubles[2] + " Last element: " + doubles[size - 1]);
    }
    // -----------------------------------------------------------------------------
    reportWorkerStatus(resourcePool, workerID, driverHost, driverPort);
    receiver.stop();
    System.exit(0);
  }

  public static <A extends Array<?>, C extends ArrCombiner<A>> void allgather(
    Workers workers, WorkerData workerData, ResourcePool resourcePool,
    ArrTable<A, C> table) {
    int workerID = workers.getSelfID();
    int[] partitionIDs = table.getPartitionIDs();
    // Gather the information of generated partitions to master
    // Generate partition and worker mapping for regrouping
    // Bcast partition regroup request
    long startTime = System.currentTimeMillis();
    LOG.info("Gather partition information.");
    ParGenAck pGenAck = new ParGenAck(workerID, partitionIDs);
    ParGenAck[][] pGenAckRef = new ParGenAck[1][];
    try {
      reqGather(workers, workerData, pGenAck, pGenAckRef, resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering data.", e);
      return;
    }
    LOG.info("All partition information are gathered.");
    long endTime = System.currentTimeMillis();
    LOG.info("Allgather start sync overhead (ms): " + (endTime - startTime));
    AllgatherReq allgatherReq = null;
    if (workers.isMaster()) {
      int totalPartitions = 0;
      ParGenAck[] pGenAcks = pGenAckRef[0];
      for (int i = 0; i < pGenAcks.length; i++) {
        int[] parIds = pGenAcks[i].getPartitionIds();
        totalPartitions = totalPartitions + parIds.length;
        // Free pGenAcks, no use in future
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(
          pGenAcks[i]);
      }
      allgatherReq = new AllgatherReq(totalPartitions);
    }
    LOG.info("Bcast allgather information.");
    // Receiver believe it is bcasted
    AllgatherReq[] allgatherReqRef = new AllgatherReq[1];
    allgatherReqRef[0] = allgatherReq;
    reqChainBcast(allgatherReqRef, workers, workerData, resourcePool,
      AllgatherReq.class);
    allgatherReq = allgatherReqRef[0];
    // ------------------------------------------------------------------------
    int numThreads = Constants.NUM_DESERIAL_THREADS;
    int totalParRecv = allgatherReq.getTotalRecvParNum();
    ArrTableCatcher<A, C> catcher = new ArrTableCatcher<A, C>(workers, workerData,
      resourcePool, totalParRecv, table, numThreads);
    catcher.waitAndGet();
    resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
      allgatherReq);
  }
}
