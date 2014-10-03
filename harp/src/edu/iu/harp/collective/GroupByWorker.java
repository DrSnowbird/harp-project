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

import java.util.List;
import java.util.Map.Entry;

import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.WorkerInfo;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.request.ParGenAck;
import edu.iu.harp.comm.request.RegroupReq;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.Receiver;
import edu.iu.harp.keyval.DoubleCombiner;
import edu.iu.harp.keyval.DoubleVal;
import edu.iu.harp.keyval.IntAvg;
import edu.iu.harp.keyval.IntCountVal;
import edu.iu.harp.keyval.IntCountValPlus;
import edu.iu.harp.keyval.Key;
import edu.iu.harp.keyval.KeyValParGetter;
import edu.iu.harp.keyval.KeyValParSender;
import edu.iu.harp.keyval.KeyValPartition;
import edu.iu.harp.keyval.KeyValTable;
import edu.iu.harp.keyval.StringKey;
import edu.iu.harp.keyval.ValCombiner;
import edu.iu.harp.keyval.ValConverter;
import edu.iu.harp.keyval.Value;

public class GroupByWorker extends CollCommWorker {
  public static void main(String args[]) throws Exception {
    String driverHost = args[0];
    int driverPort = Integer.parseInt(args[1]);
    int workerID = Integer.parseInt(args[2]);
    long jobID = Long.parseLong(args[3]);
    initLogger(workerID);
    LOG.info("args[] " + driverHost + " " + driverPort + " " + workerID + " "
      + jobID);
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
    // Generate words into wordcount table
    KeyValTable<StringKey, IntCountVal, IntCountValPlus> table = new KeyValTable<StringKey, IntCountVal, IntCountValPlus>(
      workerID, StringKey.class, IntCountVal.class, IntCountValPlus.class);
    try {
      table.addKeyVal(new StringKey("Facebook"), new IntCountVal(1, 8));
      table.addKeyVal(new StringKey("Google"), new IntCountVal(1, 6));
      table.addKeyVal(new StringKey("Amazon"), new IntCountVal(1, 6));
      table.addKeyVal(new StringKey("LinkedIn"), new IntCountVal(1, 8));
      table.addKeyVal(new StringKey("Twitter"), new IntCountVal(1, 7));
    } catch (Exception e) {
      LOG.error("Error in generating word and count.", e);
    }
    LOG.info("Word and count are generated.");
    // ------------------------------------------------------------------------
    KeyValTable<StringKey, DoubleVal, DoubleCombiner> newTable = new KeyValTable<StringKey, DoubleVal, DoubleCombiner>(
      workerID, StringKey.class, DoubleVal.class, DoubleCombiner.class);
    groupByAggregate(workers, workerData, resourcePool, table, newTable,
      new IntAvg());
    for (KeyValPartition<StringKey, IntCountVal, IntCountValPlus> partition : table
      .getKeyValPartitions()) {
      for (Entry<StringKey, IntCountVal> entry : partition.getKeyValMap()
        .entrySet()) {
        LOG.info(entry.getKey().getStringKey() + " "
          + entry.getValue().getIntValue());
      }
    }
    for (KeyValPartition<StringKey, DoubleVal, DoubleCombiner> partition : newTable
      .getKeyValPartitions()) {
      for (Entry<StringKey, DoubleVal> entry : partition.getKeyValMap()
        .entrySet()) {
        LOG.info(entry.getKey().getStringKey() + " "
          + entry.getValue().getDoubleValue());
      }
    }
    // -----------------------------------------------------------------------------
    reportWorkerStatus(resourcePool, workerID, driverHost, driverPort);
    receiver.stop();
    System.exit(0);
  }

  public static <K extends Key, V extends Value, C extends ValCombiner<V>> void groupbyCombine(
    Workers workers, WorkerData workerData, ResourcePool resourcePool,
    KeyValTable<K, V, C> table) {
    int workerID = workers.getSelfID();
    int[] partitionIDs = table.getPartitionIDs();
    int numWorkers = workers.getNumWorkers();
    // Gather the information of generated partitions to master
    // Generate partition and worker mapping for regrouping
    // Bcast partition regroup request
    ParGenAck pGenAck = new ParGenAck(workerID, partitionIDs);
    ParGenAck[][] pGenAckRef = new ParGenAck[1][];
    LOG.info("Gather partition information.");
    try {
      reqGather(workers, workerData, pGenAck, pGenAckRef, resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering data.", e);
      return;
    }
    LOG.info("All partition information are gathered.");
    RegroupReq regroupReq = null;
    if (workers.isMaster()) {
      ParGenAck[] pGenAcks = pGenAckRef[0];
      Int2IntOpenHashMap partitionWorkers = new Int2IntOpenHashMap();
      Int2IntOpenHashMap workerPartitionCount = new Int2IntOpenHashMap();
      int destWorkerID = 0;
      for (int i = 0; i < pGenAcks.length; i++) {
        int[] parIds = pGenAcks[i].getPartitionIds();
        LOG.info("Worker ID: " + pGenAcks[i].getWorkerID() + ", Partitios: "
          + parIds.length);
        for (int j = 0; j < parIds.length; j++) {
          destWorkerID = parIds[j] % numWorkers;
          partitionWorkers.put(parIds[j], destWorkerID);
          workerPartitionCount.addTo(destWorkerID, 1);
        }
      }
      LOG.info("Partition : Worker");
      for (Entry<Integer, Integer> entry : partitionWorkers.entrySet()) {
        LOG.info(entry.getKey() + " " + entry.getValue());
      }
      LOG.info("Worker : PartitionCount");
      for (Entry<Integer, Integer> entry : workerPartitionCount.entrySet()) {
        LOG.info(entry.getKey() + " " + entry.getValue());
      }
      regroupReq = new RegroupReq(partitionWorkers, workerPartitionCount);
    }
    LOG.info("Bcast regroup information.");
    // Receiver believe it is bcasted
    RegroupReq[] regroupReqRef = new RegroupReq[1];
    regroupReqRef[0] = regroupReq;
    boolean success = reqChainBcast(regroupReqRef, workers, workerData,
      resourcePool, RegroupReq.class);
    if (!success) {
      return;
    }
    LOG.info("Regroup information is bcasted.");
    regroupReq = regroupReqRef[0];
    // ------------------------------------------------------------------------
    // Send partition
    KeyValPartition<K, V, C>[] partitions = table.getKeyValPartitions();
    List<KeyValPartition<K, V, C>> remvPartitions = new ObjectArrayList<KeyValPartition<K, V, C>>();
    List<KeyValPartition<K, V, C>> recvPartitions = new ObjectArrayList<KeyValPartition<K, V, C>>();
    Int2IntOpenHashMap partitionWorkers = regroupReq.getPartitionWorkerMap();
    int localParCount = 0;
    int destWorkerID = 0;
    for (int i = 0; i < partitions.length; i++) {
      destWorkerID = partitionWorkers.get(partitions[i].getPartitionID());
      if (destWorkerID == workerID) {
        localParCount++;
      } else {
        WorkerInfo workerInfo = workers.getWorkerInfo(destWorkerID);
        KeyValParSender<K, V, C> parRegroupPut = new KeyValParSender<K, V, C>(
          workerInfo.getNode(), workerInfo.getPort(), partitions[i],
          resourcePool);
        parRegroupPut.execute();
        remvPartitions.add(partitions[i]);
      }
    }
    // ------------------------------------------------------------------------
    // Receive all the data from the queue
    Int2IntOpenHashMap workerPartitionCount = regroupReq
      .getWorkerPartitionCountMap();
    int totalPartitionsRecv = workerPartitionCount.get(workerID)
      - localParCount;
    LOG.info("Total receive: " + totalPartitionsRecv);
    KeyValParGetter<K, V, C> pRegWorker = new KeyValParGetter<K, V, C>(
      workerData, resourcePool);
    for (int i = 0; i < totalPartitionsRecv; i++) {
      KeyValPartition<K, V, C> keyValPartition = pRegWorker
        .waitAndGet(Constants.DATA_MAX_WAIT_TIME);
      recvPartitions.add(keyValPartition);
    }
    for (KeyValPartition<K, V, C> partition : remvPartitions) {
      table.removeKeyValPartition(partition.getPartitionID());
      resourcePool.getWritableObjectPool().freeWritableObjectInUse(partition);
    }
    for (KeyValPartition<K, V, C> partition : recvPartitions) {
      if (table.addKeyValPartition(partition)) {
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(partition);
      }
    }
  }

  public static <K extends Key, V1 extends Value, C1 extends ValCombiner<V1>,
    V2 extends Value, C2 extends ValCombiner<V2>, A extends ValConverter<V1, V2>>
    void convertOldTableToNewTable(
    KeyValTable<K, V1, C1> oldTable, KeyValTable<K, V2, C2> newTable,
    A converter) {
    for (KeyValPartition<K, V1, C1> partition : oldTable.getKeyValPartitions()) {
      for (Entry<K, V1> entry : partition.getKeyValMap().entrySet()) {
        newTable.addKeyVal(entry.getKey(), converter.convert(entry.getValue()));
      }
    }
  }

  public static <K extends Key, V1 extends Value, C1 extends ValCombiner<V1>, 
    V2 extends Value, C2 extends ValCombiner<V2>, A extends ValConverter<V1, V2>>
    void groupByAggregate(
    Workers workers, WorkerData workerData, ResourcePool resourcePool,
    KeyValTable<K, V1, C1> oldTable, KeyValTable<K, V2, C2> newTable,
    A converter) {
    groupbyCombine(workers, workerData, resourcePool, oldTable);
    convertOldTableToNewTable(oldTable, newTable, converter);
  }
}
