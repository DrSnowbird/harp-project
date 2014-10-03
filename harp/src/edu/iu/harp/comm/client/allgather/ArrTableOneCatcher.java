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

import org.apache.log4j.Logger;

import edu.iu.harp.arrpar.ArrCombiner;
import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.collective.RegroupWorker;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.ReqSendCenter;
import edu.iu.harp.comm.client.regroup.ArrParGetter;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.data.IntArray;
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
public class ArrTableOneCatcher<A extends Array<?>, C extends ArrCombiner<A>> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ArrTableOneCatcher.class);

  private final Workers workers;
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final ArrTable<A, C> table;

  public ArrTableOneCatcher(Workers workers, WorkerData workerData,
    ResourcePool pool, ArrTable<A, C> table) {
    this.workers = workers;
    this.workerData = workerData;
    this.pool = pool;
    this.table = table;
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
    int selfID = workers.getSelfID();
    int leftID = 0;
    int rightID = workers.getMaxID();
    int midID = (leftID + rightID) / 2;
    int half = midID - leftID + 1;
    int range = rightID - leftID + 1;
    ArrPartition<A> ownedPartition = null;
    int destID = 0;
    String destHost = null;
    int destPort = 0;
    boolean isDestAdjusted = false;
    Int2ObjectOpenHashMap<ByteArray> reltByteArrParMap = new Int2ObjectOpenHashMap<ByteArray>();
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
        isDestAdjusted= true;
      }
      destHost = workers.getWorkerInfo(destID).getNode();
      destPort = workers.getWorkerInfo(destID).getPort();
      // LOG.info("leftID " + leftID + " rightID " + rightID + " midID " + midID
      //  + " half " + half + " range " + range + " selfID " + selfID
      //  + " destID " + destID + " destHost " + destHost + " destPort "
      //  + destPort);
      // Each worker should only has one partition.
      ownedPartition = table.getPartition(0);
      // If ownedPartition is null, send an empty partition
      sendArrPartition(destHost, destPort, selfID, tableID, ownedPartition,
        table.getAClass(), pool);
      if (!isDestAdjusted) {
        success = receiveDataFromWorker(destID, tableID, reltByteArrParMap,
          skippedCommData);
        if (!success) {
          return false;
        }
      }
      // If range is odd, midID + 1 receive additional data from midID
      if (range % 2 == 1 && selfID == midID + 1) {
        // LOG.info("Get extra data");
        success = receiveDataFromWorker(midID, tableID, reltByteArrParMap,
          skippedCommData);
        if (!success) {
          return false;
        }
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
    return true;
  }

  public ArrTable<A, C> getTable() {
    return this.table;
  }

  private void sendArrPartition(String destHost, int destPort, int selfID,
    int tableID, ArrPartition<A> partition, Class<A> aClass,
    ResourcePool resourcePool) {
    if (aClass.equals(IntArray.class)) {
      @SuppressWarnings("unchecked")
      ArrPartition<IntArray> intArrPar = (ArrPartition<IntArray>) partition;
      // TO DO: send int array partition to the correct destination
    } else if (aClass.equals(DoubleArray.class)) {
      @SuppressWarnings("unchecked")
      ArrPartition<DoubleArray> dblArrPar = (ArrPartition<DoubleArray>) partition;
      if (dblArrPar == null) {
        dblArrPar = new ArrPartition<DoubleArray>(new DoubleArray(), 0);
      }
      DblArrOneSender sender = new DblArrOneSender(destHost, destPort, selfID,
        tableID, dblArrPar, resourcePool);
      sender.execute();
    } else {
      LOG.info("Cannot get correct partition sender.");
    }
  }

  private boolean receiveDataFromWorker(int workerID, int tableID,
    Int2ObjectOpenHashMap<ByteArray> reltByteArrParMap,
    ObjectArrayList<Commutable> skippedCommData) {
    Commutable data = null;
    ByteArray byteArray = reltByteArrParMap.remove(workerID);
    // Try to receive data from the same destination
    // Wait if data arrives
    if (byteArray == null) {
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
          skippedCommData.add(data);
          data = null;
          continue;
        }
        // Get the byte array
        byteArray = (ByteArray) data;
        metaArray = byteArray.getMetaArray();
        metaArraySize = byteArray.getMetaArraySize();
        if (metaArraySize != 2) {
          skippedCommData.add(data);
          data = null;
          continue;
        }
        recvWorkerID = metaArray[0];
        recvTableID = metaArray[1];
        // If this is not for this table
        if (recvTableID != tableID) {
          skippedCommData.add(data);
          data = null;
          continue;
        }
        // If not the desired worker ID, add to map
        if (recvWorkerID != workerID) {
          reltByteArrParMap.put(recvWorkerID, byteArray);
          data = null;
          continue;
        }
      } while (data == null);
    }
    // Notice the sender may send an empty partition if it doesn't have the data
    // to reduce
    byte[] bytes = byteArray.getArray();
    int size = byteArray.getSize();
    if (bytes != null && size > 0) {
      try {
        A array = ArrParGetter.desiealizeToArray(byteArray, pool,
          table.getAClass());
        ArrPartition<A> partition = new ArrPartition<A>(array, 0);
        if (table.addPartition(partition)) {
          RegroupWorker.releaseArrayPartition(partition, pool);
        }
      } catch (Exception e) {
        LOG.error("Fail to add partition to table.", e);
        return false;
      }
      pool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
    }
    pool.getIntArrayPool().releaseArrayInUse(byteArray.getMetaArray());
    return true;
  }
}
