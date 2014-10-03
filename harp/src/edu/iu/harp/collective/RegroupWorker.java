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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.Random;

import edu.iu.harp.arrpar.ArrCombiner;
import edu.iu.harp.arrpar.ArrConverter;
import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.Double2DArrAvg;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.WorkerInfo;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.regroup.ArrParGetter;
import edu.iu.harp.comm.client.regroup.DoubleArrParSender;
import edu.iu.harp.comm.client.regroup.IntArrParSender;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.request.ParGenAck;
import edu.iu.harp.comm.request.RegroupReq;
import edu.iu.harp.comm.request.ReqAck;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.Receiver;

public class RegroupWorker extends CollCommWorker {

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
    boolean success = masterHandshake(workers, workerData, resourcePool);
    LOG.info("Barrier: " + success);
    // ------------------------------------------------------------------------
    // Generate data partition
    ArrTable<DoubleArray, DoubleArrPlus> table = new ArrTable<DoubleArray, DoubleArrPlus>(
      workerID, DoubleArray.class, DoubleArrPlus.class);
    int doublesSize = partitionByteSize / 8;
    if (doublesSize < 2) {
      doublesSize = 2;
    }
    LOG.info("Double size: " + doublesSize);
    // Generate partition data
    // Assuming 1 row only
    for (int i = 0; i < numPartitions; i++) {
      // double[] doubles = new double[doublesSize];
      double[] doubles = resourcePool.getDoubleArrayPool()
        .getArray(doublesSize);
      doubles[0] = 1; // count is 1
      double num = (int) (Math.random() * 100);
      for (int j = 1; j < doublesSize; j++) {
        doubles[j] = num;
      }
      DoubleArray doubleArray = new DoubleArray();
      doubleArray.setArray(doubles);
      doubleArray.setSize(doublesSize);
      ArrPartition<DoubleArray> partition = new ArrPartition<DoubleArray>(doubleArray, i);
      LOG.info("Data Generate, WorkerID: " + workerID + " Partition: "
        + partition.getPartitionID() + 
        " Count: " + doubles[0] + " First element: " + doubles[1]
        + " Last element: " + doubles[doublesSize - 1]);
      table.addPartition(partition);
    }
    // ------------------------------------------------------------------------
    // Regroup
    ArrTable<DoubleArray, DoubleArrPlus> newTable = new ArrTable<DoubleArray, DoubleArrPlus>(
      workerID, DoubleArray.class, DoubleArrPlus.class);
    /*
    regroupCombine(workers, workerData, resourcePool, tableRef);
    table = tableRef[0];
    for (ArrPartition<DoubleArray> partition : table.getPartitions()) {
      double[] doubles = partition.getArray().getArray();
      int size = partition.getArray().getSize();
      LOG.info(" Partition: " + partition.getPartitionID() + " Count: "
        + doubles[0] + " First element: " + doubles[1] + " Last element: "
        + doubles[size - 1]);
    }
    */
    // regroupAggregate(workers, workerData, resourcePool, tableRef, newTableRef, new Double2DArrAvg());
    long startTime = System.currentTimeMillis();
    allreduce(workers, workerData, resourcePool, table, newTable,
      new Double2DArrAvg(doublesSize));
    long endTime = System.currentTimeMillis();
    LOG.info("All reduce time cost: " + (endTime - startTime));
    for (ArrPartition<DoubleArray> partition : newTable.getPartitions()) {
      double[] doubles = partition.getArray().getArray();
      int size = partition.getArray().getSize();
      LOG.info(" Partition: " + partition.getPartitionID() + " Count: "
        + doubles[0] + " First element: " + doubles[1] + " Last element: "
        + doubles[size - 1]);
    }
    
    // -----------------------------------------------------------------------------
    reportWorkerStatus(resourcePool, workerID, driverHost, driverPort);
    receiver.stop();
    System.exit(0);
  }
  
  public static <A extends Array<?>, C extends ArrCombiner<A>> void regroupCombine(
    Workers workers, WorkerData workerData, ResourcePool resourcePool,
    ArrTable<A, C> table) throws Exception {
    if (workers.getNumWorkers() <= 1) {
      return;
    }
    int workerID = workers.getSelfID();
    int[] partitionIDs = table.getPartitionIDs();
    int numWorkers = workers.getNumWorkers();
    boolean success = false;
    // Gather the information of generated partitions to master
    // Generate partition and worker mapping for regrouping
    // Bcast partition regroup request
    // LOG.info("Gather partition information.");
    long startTime = System.currentTimeMillis();
    ParGenAck pGenAck = new ParGenAck(workerID, partitionIDs);
    ParGenAck[][] pGenAckRef = new ParGenAck[1][];
    try {
      success = reqGather(workers, workerData, pGenAck, pGenAckRef,
        resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering data.", e);
      return;
    }
    long endTime = System.currentTimeMillis();
    // LOG.info("All partition information are gathered.");
    LOG.info("Regroup-combine sync overhead (ms): " + (endTime - startTime));
    if (!success) {
      throw new Exception("Fail to gather data.");
    }
    RegroupReq[] regroupReqRef = new RegroupReq[1];
    RegroupReq regroupReq = null;
    if (workers.isMaster()) {
      ParGenAck[] pGenAcks = pGenAckRef[0];
      Int2IntOpenHashMap partitionWorkers = new Int2IntOpenHashMap();
      Int2IntOpenHashMap workerPartitionCount = new Int2IntOpenHashMap();
      int destWorkerID = 0;
      for (int i = 0; i < pGenAcks.length; i++) {
        int[] parIds = pGenAcks[i].getPartitionIds();
        for (int j = 0; j < parIds.length; j++) {
          destWorkerID = parIds[j] % numWorkers;
          partitionWorkers.put(parIds[j], destWorkerID);
          workerPartitionCount.addTo(destWorkerID, 1);
        }
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(
          pGenAcks[i]);
      }
      // LOG.info("Partition : Worker");
      // for (Entry<Integer, Integer> entry : partitionWorkers.entrySet()) {
      // LOG.info(entry.getKey() + " " + entry.getValue());
      // }
      // LOG.info("Worker : PartitionCount");
      // for (Entry<Integer, Integer> entry : workerPartitionCount.entrySet()) {
      // LOG.info(entry.getKey() + " " + entry.getValue());
      // }
      regroupReq = new RegroupReq(partitionWorkers, workerPartitionCount);
      regroupReqRef[0] = regroupReq;
    }
    pGenAck = null;
    pGenAckRef = null;
    partitionIDs = null;
    // ------------------------------------------------------------------------
    // Bcast
    // LOG.info("Bcast regroup information.");
    success = reqChainBcast(regroupReqRef, workers, workerData, resourcePool,
      RegroupReq.class);
    if (!success) {
      throw new Exception("Fail to bcast data.");
    }
    regroupReq = regroupReqRef[0];
    // LOG.info("Regroup information is bcasted.");
    // ------------------------------------------------------------------------
    // Send partition
    ArrPartition<A>[] partitions = table.getPartitions();
    Int2IntOpenHashMap partitionWorkers = regroupReq.getPartitionWorkerMap();
    ObjectArrayList<ArrPartition<A>> remvPartitions = new ObjectArrayList<ArrPartition<A>>();
    ObjectArrayList<ArrPartition<A>> recvPartitions = new ObjectArrayList<ArrPartition<A>>();
    int localParCount = 0;
    int destWorkerID = 0;
    int[] order = createRandomNumersInRange(System.nanoTime() / 1000 * 1000
      + workerID, partitions.length);
    for (int i = 0; i < order.length; i++) {
      destWorkerID = partitionWorkers
        .get(partitions[order[i]].getPartitionID());
      if (destWorkerID == workerID) {
        localParCount++;
      } else {
        WorkerInfo workerInfo = workers.getWorkerInfo(destWorkerID);
        sendArrayPartition(workerInfo.getNode(), workerInfo.getPort(),
          partitions[order[i]], resourcePool);
        remvPartitions.add(partitions[order[i]]);
      }
    }
    partitions = null;
    // ------------------------------------------------------------------------
    // Receive all the data from the queue
    Int2IntOpenHashMap workerPartitionCount = regroupReq
      .getWorkerPartitionCountMap();
    int totalPartitionsRecv = workerPartitionCount.get(workerID)
      - localParCount;
    // LOG.info("Total receive: " + totalPartitionsRecv);
    ArrParGetter<A> pRegWorker = new ArrParGetter<A>(workerData, resourcePool,
      table.getAClass());
    for (int i = 0; i < totalPartitionsRecv; i++) {
      ArrPartition<A> arrayPartition = pRegWorker
        .waitAndGet(Constants.DATA_MAX_WAIT_TIME);
      recvPartitions.add(arrayPartition);
    }
    // Remove partitions sent out
    for (ArrPartition<A> partition : remvPartitions) {
      table.removePartition(partition.getPartitionID());
      releaseArrayPartition(partition, resourcePool);
    }
    remvPartitions = null;
    // Add received partitions
    for (ArrPartition<A> partition : recvPartitions) {
      if (table.addPartition(partition)) {
        // If combine happens, release the partition
        releaseArrayPartition(partition, resourcePool);
      }
    }
    recvPartitions = null;
    // Free regroupReq, no use in future
    resourcePool.getWritableObjectPool().freeWritableObjectInUse(regroupReq);
    regroupReqRef = null;
    regroupReq = null;
    // LOG.info("Start collect regroup finishing information.");
    startTime = System.currentTimeMillis();
    ReqAck reqAck = new ReqAck(workerID, 0);
    ReqAck[][] reqAckRef = new ReqAck[1][];
    try {
      reqCollect(workers, workerData, reqAck, reqAckRef, resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering data.", e);
      return;
    }
    if (workers.isMaster()) {
      ReqAck[] reqAcks = reqAckRef[0];
      for (int i = 0; i < reqAcks.length; i++) {
        resourcePool.getWritableObjectPool()
          .freeWritableObjectInUse(reqAcks[i]);
      }
    }
    endTime = System.currentTimeMillis();
    // LOG.info("All regroup finishing information are collected.");
    LOG
      .info("Regroup-combine end sync overhead (ms): " + (endTime - startTime));
  }
  
  @SuppressWarnings("unchecked")
  private static <A extends Array<?>> void sendArrayPartition(String host,
    int port, ArrPartition<A> partition, ResourcePool resourcePool) {
    if (partition.getArray().getClass().equals(IntArray.class)) {
      IntArrParSender parRegroupPut = new IntArrParSender(host, port,
        resourcePool, (ArrPartition<IntArray>) partition);
      parRegroupPut.execute();
    } else if (partition.getArray().getClass().equals(DoubleArray.class)) {
      ArrPartition<DoubleArray> doubleArrPar = (ArrPartition<DoubleArray>) partition;
      DoubleArrParSender parRegroupPut = new DoubleArrParSender(host, port,
        resourcePool, doubleArrPar);
      parRegroupPut.execute();
    } else {
      LOG.info("Cannot get correct partition sender.");
    }
  }
  
  public static <A extends Array<?>> void releaseArrayPartition(
    ArrPartition<A> partition, ResourcePool resourcePool) {
    A array = partition.getArray();
    if (array.getClass().equals(IntArray.class)) {
      IntArray intArray = (IntArray) array;
      resourcePool.getIntArrayPool().releaseArrayInUse(intArray.getArray());
    } else if (array.getClass().equals(DoubleArray.class)) {
      DoubleArray doubleArray = (DoubleArray) array;
      // LOG.info("Release double array with size: "
      //  + doubleArray.getArray().length);
      resourcePool.getDoubleArrayPool().releaseArrayInUse(
        doubleArray.getArray());
    } else {
      LOG.info("Cannot release the array partition.");
    }
  }
  
  public static <A1 extends Array<?>, C1 extends ArrCombiner<A1>, 
    A2 extends Array<?>, C2 extends ArrCombiner<A2>, C extends ArrConverter<A1, A2>> 
    void convertOldTableToNewTable(
    ArrTable<A1, C1> oldTable, ArrTable<A2, C2> newTable, C converter) throws Exception {
    for (ArrPartition<A1> partition : oldTable.getPartitions()) {
      // Notice that the old partition may modified after conversion
      newTable.addPartition(converter.convert(partition));
    }
  }

  public static <A1 extends Array<?>, C1 extends ArrCombiner<A1>, 
    A2 extends Array<?>, C2 extends ArrCombiner<A2>,
    C extends ArrConverter<A1, A2>> void regroupAggregate(
    Workers workers, WorkerData workerData, ResourcePool resourcePool,
    ArrTable<A1, C1> oldTable, ArrTable<A2, C2> newTable, C converter)
    throws Exception {
    regroupCombine(workers, workerData, resourcePool, oldTable);
    convertOldTableToNewTable(oldTable, newTable, converter);
  }
  
  public static <A1 extends Array<?>, C1 extends ArrCombiner<A1>,
    A2 extends Array<?>, C2 extends ArrCombiner<A2>,
    C extends ArrConverter<A1, A2>> void allreduce(
    Workers workers, WorkerData workerData, ResourcePool resourcePool,
    ArrTable<A1, C1> oldTable, ArrTable<A2, C2> newTable, C converter)
    throws Exception {
    long time1 = System.currentTimeMillis();
    regroupAggregate(workers, workerData, resourcePool, oldTable, newTable,
      converter);
    long time2 = System.currentTimeMillis();
    AllgatherWorker.allgather(workers, workerData, resourcePool, newTable);
    long time3 = System.currentTimeMillis();
    LOG.info("Regroup-aggregate time (ms): " + (time2 - time1)
      + " Allgather time (ms): " + (time3 - time2));
  }
  
  public static int[] createRandomNumersInRange(long seed, int max) {
    Random random = new Random(seed);
    int[] num = new int[max];
    Arrays.fill(num, -1);
    int next = 0;
    int pos = -1;
    for (int i = 0; i < max; i++) {
      do {
        pos = -1;
        next = random.nextInt(max);
        for (int j = 0; j < i; j++) {
          if (num[j] == next) {
            pos = j;
            break;
          }
        }
      } while (pos >= 0);
      num[i] = next;
      // LOG.info("num_" + i + "=" + num[i]);
      // System.out.println("num_" + i + "=" + num[i]);
    }
    return num;
  }
}
