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
import edu.iu.harp.comm.client.ReqSendCenter;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class ArrTableMultiThreadCatcher<A extends Array<?>, C extends ArrCombiner<A>> {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(ArrTableMultiThreadCatcher.class);

  private final Workers workers;
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final int totalNumPartitions;
  private final ArrTable<A, C> table;
  private final int numDeserialThreads;

  public ArrTableMultiThreadCatcher(Workers workers, WorkerData workerData,
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
    // If there is only one worker,
    // no allgather
    if (workers.getNumWorkers() == 1) {
      return true;
    }
    // Send the partitions owned by this worker
    // long time1 = System.currentTimeMillis();
    int tableID = this.table.getTableID();
    ArrPartition<A>[] ownedPartitions = table.getPartitions();
    int numOwnedPartitions = ownedPartitions.length;
    for (int i = 0; i < numOwnedPartitions; i++) {
      deliverArrPartitionInThreads(workers, tableID, ownedPartitions[i], pool);
    }
    // Get other partitions from other workers
    // long time2 = System.currentTimeMillis();
    int nextWorkerID = workers.getNextID();
    String nextHost = workers.getNextInfo().getNode();
    int nextPort = workers.getNextInfo().getPort();
    int numExpPartitions = totalNumPartitions - numOwnedPartitions;
    ObjectArrayList<ByteArray> recvByteArrayPartitions = new ObjectArrayList<ByteArray>(
      numExpPartitions);
    ObjectArrayList<Commutable> skippedCommData = new ObjectArrayList<Commutable>();
    Commutable data = null;
    ByteArray byteArray = null;
    int[] metaArray = null;
    int recvWorkerID = 0;
    int recvTableID = 0;
    ByteArrReqSender byteArraySender = null;
    for (int i = numOwnedPartitions; i < totalNumPartitions; i++) {
      // Wait if data arrives
      data = this.workerData.waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data == null) {
        // LOG.info("No Data");
        return false;
      }
      // Skip unnecessary data
      if (!(data instanceof ByteArray)) {
        skippedCommData.add(data);
        i--;
        // LOG.info("Not ByteArray");
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
        // LOG.info("Not in Table");
        continue;
      }
      // Continue sending to your next neighbor
      if (recvWorkerID != nextWorkerID) {
        byteArraySender = new ByteArrReqSender(nextHost, nextPort, byteArray,
          pool);
        ReqSendCenter.submit(byteArraySender);
      }
      recvByteArrayPartitions.add(byteArray);
    }
    if (!skippedCommData.isEmpty()) {
      workerData.putAllCommData(skippedCommData);
    }
    // Deserialize
    // long time3 = System.currentTimeMillis();
    List<ArrPartition<A>> partitions = CollCommWorker.doTasks(
      recvByteArrayPartitions, "allgather-deserialize-executor",
      new ArrDeserialTask<A>(pool, table.getAClass()), numDeserialThreads);
    for (ArrPartition<A> partition : partitions) {
      try {
        if (table.addPartition(partition)) {
          RegroupWorker.releaseArrayPartition(partition, this.pool);
        }
      } catch (Exception e) {
        LOG.error("Fail to add partition to table.", e);
        return false;
      }
    }
    // Must finish sending, otherwise data may be released but not sent
    // long time4 = System.currentTimeMillis();
    ReqSendCenter.waitForFinish();
    // Release
    // long time5 = System.currentTimeMillis();
    for (ByteArray byteArr : recvByteArrayPartitions) {
      pool.getByteArrayPool().releaseArrayInUse(byteArr.getArray());
      pool.getIntArrayPool().releaseArrayInUse(byteArr.getMetaArray());
    }
    recvByteArrayPartitions = null;
    // long time6 = System.currentTimeMillis();
    // LOG.info((time2 - time1) + " " + (time3 - time2) + " " + (time4 - time3)
    // + " " + (time5 - time4) + " " + (time6 - time5));
    return true;
  }

  public ArrTable<A, C> getTable() {
    return this.table;
  }

  private void deliverArrPartitionInThreads(Workers workers, int tableID,
    ArrPartition<A> partition, ResourcePool resourcePool) {
    if (partition.getArray().getClass().equals(IntArray.class)) {
      @SuppressWarnings("unchecked")
      ArrPartition<IntArray> intArrPar = (ArrPartition<IntArray>) partition;
      IntArrParDeliver deliver = new IntArrParDeliver(workers, resourcePool,
        tableID, intArrPar);
      ReqSendCenter.submit(deliver);
    } else if (partition.getArray().getClass().equals(DoubleArray.class)) {
      @SuppressWarnings("unchecked")
      ArrPartition<DoubleArray> doubleArrPar = (ArrPartition<DoubleArray>) partition;
      DblArrParDeliver deliver = new DblArrParDeliver(workers, resourcePool,
        tableID, doubleArrPar);
      ReqSendCenter.submit(deliver);
    } else {
      LOG.info("Cannot get correct partition deliver.");
    }
  }
}
