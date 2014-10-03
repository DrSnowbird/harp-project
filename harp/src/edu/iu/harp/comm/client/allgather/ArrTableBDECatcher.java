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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
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
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;

/**
 * If allgather is for some combinable data which even distributed on all ther
 * workers. The total data size will go up as the number of workers increases.
 * In this situation, we use bi-directional method other than bucket method.
 * 
 * @author zhangbj
 * 
 * @param <A>
 * @param <C>
 */
public class ArrTableBDECatcher<A extends Array<?>, C extends ArrCombiner<A>> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ArrTableBDECatcher.class);

  private final Workers workers;
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final ArrTable<A, C> table;
  private final int numDeserialThreads;

  public ArrTableBDECatcher(Workers workers, WorkerData workerData,
    ResourcePool pool, ArrTable<A, C> table, int numDeserialThreads) {
    this.workers = workers;
    this.workerData = workerData;
    this.pool = pool;
    this.table = table;
    this.numDeserialThreads = numDeserialThreads;
  }

  public boolean waitAndGet() {
    // If there is only one worker,
    // no allgather
    if (workers.getNumWorkers() == 1) {
      return true;
    }
    // Initially each worker only owns one partition.
    // Here we assume that
    // worker ID starts from 0 to totalNumWorkers - 1
    int tableID = table.getTableID();
    // positions tagged by IDs
    int numWorkers = workers.getNumWorkers();
    int selfID = workers.getSelfID();
    int leftID = 0;
    int rightID = workers.getMaxID();
    int midID = (leftID + rightID) / 2;
    int half = midID - leftID + 1;
    int range = rightID - leftID + 1;
    // Serialize all the partitions owned by this worker to byte array
    ArrPartition<A>[] ownedPartitions = table.getPartitions();
    ByteArray byteArray = null;
    try {
      byteArray = ArrTableMultiParCatcher.serializeMultiArrPartitions(selfID,
        tableID, ownedPartitions, pool);
    } catch (Exception e) {
      LOG.error("Fail to serialize multi array partitions.", e);
      return false;
    }
    int destID = 0;
    String destHost = null;
    int destPort = 0;
    boolean isDestAdjusted = false;
    // Record the worker IDs this worker receives data from in the last step
    int lastRecvWorkerID = -1;
    int lastExtraRecvWorkerID = -1;
    Int2ObjectOpenHashMap<ByteArray> recvByteArrParMap = new Int2ObjectOpenHashMap<ByteArray>(
      numWorkers - 1);
    ObjectArrayList<Commutable> skippedCommData = new ObjectArrayList<Commutable>();
    boolean success = true;
    while (leftID != rightID) {
      if (selfID <= midID) {
        destID = selfID + half;
      } else {
        destID = selfID - half;
      }
      // If the range is odd, midID's destID will be out of range.
      if (destID > rightID) {
        // LOG.info("Dest adjusted, original " + destID);
        destID = midID + 1;
        isDestAdjusted = true;
      }
      destHost = workers.getWorkerInfo(destID).getNode();
      destPort = workers.getWorkerInfo(destID).getPort();
      // LOG.info("leftID " + leftID + " rightID " + rightID + " midID " + midID
      //  + " half " + half + " range " + range + " selfID " + selfID
      //  + " destID " + destID + " destHost " + destHost + " destPort "
      //  + destPort);
      // Get all the partitions and serialize them into one byte array
      // In later iteration, combine what are serialized, with what
      // received in the last iteration, send to the current destination
      // try to remember a current byte array, once receive and try to merge
      // them together
      if (lastRecvWorkerID != -1 || lastExtraRecvWorkerID != -1) {
        // Combine byte array with byte array received from lastRecvWorkerID and
        // lastExtraRecvWorkerID to get a new byte array
        mergeByteArray(byteArray, recvByteArrParMap, lastRecvWorkerID,
          lastExtraRecvWorkerID);
        lastRecvWorkerID = -1;
        lastExtraRecvWorkerID = -1;
        // LOG.info("Data is merged.");
      }
      sendByteArray(destHost, destPort, selfID, byteArray);
      // For destination adjusted worker, it doesn't need to receive data
      if (!isDestAdjusted) {
        // LOG.info("Get data");
        success = receiveDataFromWorker(destID, tableID, recvByteArrParMap,
          skippedCommData);
        if (!success) {
          pool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
          byteArray = null;
          return false;
        }
        lastRecvWorkerID = destID;
      }
      // If range is odd, midID + 1 receive additional data from midID
      if (range % 2 == 1 && selfID == midID + 1) {
        // LOG.info("Get extra data");
        success = receiveDataFromWorker(midID, tableID, recvByteArrParMap,
          skippedCommData);
        if (!success) {
          pool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
          byteArray = null;
          return false;
        }
        lastExtraRecvWorkerID = midID;
      }
      if (selfID <= midID) {
        rightID = midID;
      } else {
        leftID = midID + 1;
      }
      midID = (leftID + rightID) / 2;
      half = midID - leftID + 1;
      range = rightID - leftID + 1;
      isDestAdjusted = false;
    }
    if (!skippedCommData.isEmpty()) {
      workerData.putAllCommData(skippedCommData);
    }
    // Deserialize data in recvByteArrParMap in parallel
    // Add them to array table
    ByteArray[] recvByteArrays = new ByteArray[recvByteArrParMap.size()];
    recvByteArrParMap.values().toArray(recvByteArrays);
    List<ArrPartition<A>[]> multiPartitions = CollCommWorker.doTasks(
      recvByteArrays, "allgather-deserialize-executor",
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
    // Must finish sending, then we can start do de-serialization
    // Otherwise data may be released but not sent
    // ReqSendCenter.waitForFinish();
    // Release
    for (ByteArray byteArr : recvByteArrays) {
      pool.getByteArrayPool().releaseArrayInUse(byteArr.getArray());
      pool.getIntArrayPool().releaseArrayInUse(byteArr.getMetaArray());
    }
    pool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
    pool.getIntArrayPool().releaseArrayInUse(byteArray.getMetaArray());
    return true;
  }

  public ArrTable<A, C> getTable() {
    return this.table;
  }

  private void mergeByteArray(ByteArray byteArray,
    Int2ObjectOpenHashMap<ByteArray> recvByteArrParMap, int lastRecvWorkerID,
    int lastExtraRecvWorkerID) {
    int size = 0;
    byte[] bytes0 = null;
    int size0 = 0;
    if (byteArray.getArray() != null && byteArray.getSize() > 0) {
      bytes0 = byteArray.getArray();
      size0 = byteArray.getSize();
      size += size0;
    }
    // Get the byte array from lastRecvWorkerID
    ByteArray byteArray1 = null;
    byte[] bytes1 = null;
    int size1 = 0;
    if (lastRecvWorkerID != -1) {
      // should not be null
      byteArray1 = recvByteArrParMap.get(lastRecvWorkerID);
      if (byteArray1.getArray() != null && byteArray1.getSize() > 0) {
        bytes1 = byteArray1.getArray();
        size1 = byteArray1.getSize();
        size += size1;
      }
    }
    // Get the byte array from lastExtraRecvWorkerID
    ByteArray byteArray2 = null;
    byte[] bytes2 = null;
    int size2 = 0;
    if (lastExtraRecvWorkerID != -1) {
      // should not be null
      byteArray2 = recvByteArrParMap.get(lastExtraRecvWorkerID);
      if (byteArray2.getArray() != null && byteArray2.getSize() > 0) {
        bytes2 = byteArray2.getArray();
        size2 = byteArray2.getSize();
        size += size2;
      }
    }
    // bytes0, bytes1 and bytes2 are arrays independent to each other
    // all with start 0.
    if (size1 == 0 && size2 == 0) {
      // Byte array is not changed...
      return;
    }
    if (bytes0 != null && bytes0.length >= size) {
      int curSize = size0;
      if (size1 > 0) {
        System.arraycopy(bytes1, 0, bytes0, curSize, size1);
        curSize += size1;
      }
      if (size2 > 0) {
        System.arraycopy(bytes2, 0, bytes0, curSize, size2);
        curSize += size2;
      }
      byteArray.setSize(size);
    } else {
      byte[] bytes = pool.getByteArrayPool().getArray(size);
      int curSize = 0;
      // Copy data to this new array
      if (size0 > 0) {
        System.arraycopy(bytes0, 0, bytes, curSize, size0);
        curSize += size0;
      }
      if (size1 > 0) {
        System.arraycopy(bytes1, 0, bytes, curSize, size1);
        curSize += size1;
      }
      if (size2 > 0) {
        System.arraycopy(bytes2, 0, bytes, curSize, size2);
        curSize += size2;
      }
      // Release original byte array
      if (byteArray.getArray() != null) {
        pool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
      }
      // Put new byte array
      byteArray.setArray(bytes);
      byteArray.setSize(size);
    }
    // Modify partition length
    if (byteArray1 != null) {
      byteArray.getMetaArray()[2] += byteArray1.getMetaArray()[2];
    }
    if (byteArray2 != null) {
      byteArray.getMetaArray()[2] += byteArray2.getMetaArray()[2];
    }
  }

  private void sendByteArray(String destHost, int destPort, int selfID,
    ByteArray byteArray) {
    if (byteArray.getArray() != null && byteArray.getSize() > 0) {
      ByteArrReqSender byteArraySender = new ByteArrReqSender(destHost,
        destPort, byteArray, pool);
      byteArraySender.execute();
      // LOG.info("Data is sent.");
      // No release data, for combining at the next step
    }
  }

  /**
   * Receive a byte array from another worker with worker ID required in
   * bi-directional algorithm.
   * 
   * @param workerID
   * @param tableID
   * @param recvByteArrParMap
   * @param skippedCommData
   * @return
   */
  private boolean receiveDataFromWorker(int workerID, int tableID,
    Int2ObjectOpenHashMap<ByteArray> recvByteArrParMap,
    ObjectArrayList<Commutable> skippedCommData) {
    if (!recvByteArrParMap.containsKey(workerID)) {
      Commutable data = null;
      ByteArray byteArray = null;
      int[] metaArray = null;
      int metaArraySize = 0;
      int recvWorkerID = 0;
      int recvTableID = 0;
      do {
        data = workerData.waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
        // If no data arrives, we have to return false.
        if (data == null) {
          return false;
        }
        // Skip unnecessary data
        if (!(data instanceof ByteArray)) {
          // LOG.info("Not ByteArray");
          skippedCommData.add(data);
          data = null;
          continue;
        }
        // Get the byte array
        byteArray = (ByteArray) data;
        metaArray = byteArray.getMetaArray();
        metaArraySize = byteArray.getMetaArraySize();
        if (metaArraySize != 3) {
          // LOG.info("MetaArray length is not 3: " + metaArray.length);
          skippedCommData.add(data);
          data = null;
          continue;
        }
        recvWorkerID = metaArray[0];
        recvTableID = metaArray[1];
        // If this is not for this table
        if (recvTableID != tableID) {
          // LOG.info("Not the same table: " + recvTableID);
          skippedCommData.add(data);
          data = null;
          continue;
        }
        // If not the desired worker ID, add to map
        if (recvWorkerID != workerID) {
          // LOG.info("Not from the worker desired: " + workerID + " "
          // + recvWorkerID);
          recvByteArrParMap.put(recvWorkerID, byteArray);
          data = null;
          continue;
        }
        // The correct data we need for next loop
        recvByteArrParMap.put(recvWorkerID, byteArray);
      } while (data == null);
    }
    return true;
  }
}
