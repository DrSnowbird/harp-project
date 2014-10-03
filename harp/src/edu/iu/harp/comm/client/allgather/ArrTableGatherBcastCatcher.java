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

package edu.iu.harp.comm.client.allgather;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

import org.apache.log4j.Logger;

import edu.iu.harp.arrpar.ArrCombiner;
import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.collective.CollCommWorker;
import edu.iu.harp.collective.RegroupWorker;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.ByteArrReqSender;
import edu.iu.harp.comm.client.chainbcast.ByteArrChainBcastMaster;
import edu.iu.harp.comm.client.chainbcast.ChainBcastMaster;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.DataSerializer;
import edu.iu.harp.comm.resource.ResourcePool;

public class ArrTableGatherBcastCatcher<A extends Array<?>, C extends ArrCombiner<A>> {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(ArrTableGatherBcastCatcher.class);

  private final Workers workers;
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final int totalNumPartitions;
  private final ArrTable<A, C> table;
  private final int numDeserialThreads;

  public ArrTableGatherBcastCatcher(Workers workers, WorkerData workerData,
    ResourcePool pool, int totalNumPartitions, ArrTable<A, C> table,
    int numDeserialThreads) {
    this.workers = workers;
    this.workerData = workerData;
    this.pool = pool;
    this.table = table;
    this.totalNumPartitions = totalNumPartitions;
    this.numDeserialThreads = numDeserialThreads;
  }

  public boolean waitAndGet() {
    if (workers.getNumWorkers() == 1) {
      return true;
    }
    // Get table info
    int tableID = table.getTableID();
    ArrPartition<A>[] ownedPartitions = table.getPartitions();
    // Get worker info
    int numWorkers = workers.getNumWorkers();
    int numSlaves = numWorkers - 1;
    int workerID = workers.getSelfID();
    boolean isMaster = workers.isMaster();
    ObjectArrayList<ByteArray> recvByteArrayPartitions = new ObjectArrayList<ByteArray>(
      numWorkers);
    ObjectArrayList<Commutable> skippedCommData = new ObjectArrayList<Commutable>();
    ByteArray byteArray = null;
    // Try to serialize the partitions owned
    // The owned partitions for each worker should be > 0
    if (ownedPartitions.length > 0) {
      try {
        byteArray = serializeMultiArrPartitions(workerID, tableID,
          ownedPartitions);
      } catch (Exception e) {
        LOG.error("Fail to serialize multi array partitions.", e);
        return false;
      }
    } else {
      return false;
    }
    // LOG.info("Owned data are serialized");
    // Gather data to master
    if (!isMaster) {
      // If not master, send the partitions to the master
      String masterHost = workers.getMasterInfo().getNode();
      int masterPort = workers.getMasterInfo().getPort();
      ByteArrReqSender byteArraySender = null;
      if (byteArray != null) {
        byteArraySender = new ByteArrReqSender(masterHost, masterPort,
          byteArray, pool);
        byteArraySender.execute();
        // Release byte array
        pool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
        pool.getIntArrayPool().releaseArrayInUse(byteArray.getMetaArray());
        byteArray = null;
      }
    } else {
      recvByteArrayPartitions.add(byteArray);
      recvData(recvByteArrayPartitions, skippedCommData, workerID, tableID,
        numSlaves);
    }
    // LOG.info("recvByteArrayPartitions size " + recvByteArrayPartitions.size());
    // Bcast
    if (isMaster) {
      ChainBcastMaster bcastDriver = null;
      for (ByteArray byteArr : recvByteArrayPartitions) {
        try {
          bcastDriver = new ByteArrChainBcastMaster(byteArr, workers, pool);
        } catch (Exception e) {
          return false;
        }
        bcastDriver.execute();
      }
      // remove the byte array we added before broadcasting
      recvByteArrayPartitions.remove(byteArray);
      pool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
      pool.getIntArrayPool().releaseArrayInUse(byteArray.getMetaArray());
      byteArray = null;
    } else {
      recvData(recvByteArrayPartitions, skippedCommData, workerID, tableID,
        numWorkers);
    }
    // LOG.info("recvByteArrayPartitions size " + recvByteArrayPartitions.size());
    // Deserialize
    List<ArrPartition<A>[]> multiPartitions = CollCommWorker.doTasks(
      recvByteArrayPartitions, "allgather-deserialize-executor",
      new MultiArrDeserialTask<A>(pool, table.getAClass()), numDeserialThreads);
    for (ArrPartition<A>[] multiPartition : multiPartitions) {
      for (ArrPartition<A> partition : multiPartition) {
        try {
          if (table.addPartition(partition)) {
            RegroupWorker.releaseArrayPartition(partition, pool);
          }
        } catch (Exception e) {
          LOG.error("Fail to add partition to table.", e);
          return false;
        }
      }
    }
    // Release
    for (ByteArray byteArr : recvByteArrayPartitions) {
      pool.getByteArrayPool().releaseArrayInUse(byteArr.getArray());
      pool.getIntArrayPool().releaseArrayInUse(byteArr.getMetaArray());
    }
    recvByteArrayPartitions = null;
    skippedCommData = null;
    return true;
  }

  public ArrTable<A, C> getTable() {
    return table;
  }

  private ByteArray serializeMultiArrPartitions(int workerID, int tableID,
    ArrPartition<A>[] partitions) throws Exception {
    int size = 0;
    byte[] bytes = null;
    // Get total byte size
    if (partitions[0].getArray().getClass().equals(DoubleArray.class)) {
      @SuppressWarnings("unchecked")
      ArrPartition<DoubleArray>[] dblArrPartitions = (ArrPartition<DoubleArray>[]) partitions;
      size = 4; // number of partitions
      for (ArrPartition<DoubleArray> dblArrPartition : dblArrPartitions) {
        // partition ID and array size, each double is 8 bytes
        size = size + 8 + dblArrPartition.getArray().getSize() * 8;
      }
      // Use resource pool to get a large byte array
      bytes = pool.getByteArrayPool().getArray(size);
      double[] doubles = null;
      int arrSize = 0;
      DataSerializer serializer = new DataSerializer(bytes);
      // Write number of partitions
      serializer.writeInt(dblArrPartitions.length);
      for (ArrPartition<DoubleArray> dblArrPartition : dblArrPartitions) {
        // Write partition ID
        serializer.writeInt(dblArrPartition.getPartitionID());
        doubles = dblArrPartition.getArray().getArray();
        arrSize = dblArrPartition.getArray().getSize();
        // Write array size
        serializer.writeInt(arrSize);
        // Here arrays are assumed to start from 0
        for (int i = 0; i < arrSize; i++) {
          serializer.writeDouble(doubles[i]);
        }
      }
    } else {
      throw new Exception("Cannot serialize partitions.");
    }
    ByteArray byteArray = new ByteArray();
    byteArray.setArray(bytes);
    int[] metaArray = pool.getIntArrayPool().getArray(2);
    metaArray[0] = workerID;
    metaArray[1] = tableID;
    byteArray.setMetaArray(metaArray);
    byteArray.setMetaArraySize(2);
    byteArray.setSize(size);
    byteArray.setStart(0);
    return byteArray;
  }

  private boolean recvData(ObjectArrayList<ByteArray> recvByteArrayPartitions,
    ObjectArrayList<Commutable> skippedCommData, int workerID, int tableID,
    int numTotalRecv) {
    Commutable data = null;
    ByteArray byteArray = null;
    int[] metaArray = null;
    int recvWorkerID = 0;
    int recvTableID = 0;
    for (int i = 0; i < numTotalRecv; i++) {
      // Wait if data arrives
      data = workerData.waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data == null) {
        if (!skippedCommData.isEmpty()) {
          workerData.putAllCommData(skippedCommData);
        }
        return false;
      }
      // Skip unnecessary data
      if (!(data instanceof ByteArray)) {
        skippedCommData.add(data);
        i--;
        continue;
      }
      // Get the byte array
      byteArray = (ByteArray) data;
      metaArray = byteArray.getMetaArray();
      recvWorkerID = metaArray[0];
      recvTableID = metaArray[1];
      // If this is not for this table
      if (recvTableID != tableID) {
        skippedCommData.add(data);
        i--;
        continue;
      }
      // Since we already have the data, release directly
      if (recvWorkerID == workerID) {
        pool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
        pool.getIntArrayPool().releaseArrayInUse(byteArray.getMetaArray());
        byteArray = null;
        continue;
      }
      recvByteArrayPartitions.add(byteArray);
    }
    if (!skippedCommData.isEmpty()) {
      workerData.putAllCommData(skippedCommData);
    }
    return true;
  }
}
