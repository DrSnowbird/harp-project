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

import org.apache.log4j.Logger;

import edu.iu.harp.arrpar.ArrCombiner;
import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.collective.RegroupWorker;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.ByteArrReqSender;
import edu.iu.harp.comm.client.regroup.ArrParGetter;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class ArrTableCatcher<A extends Array<?>, C extends ArrCombiner<A>> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ArrTableCatcher.class);

  private final Workers workers;
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final int totalNumPartitions;
  private final ArrTable<A, C> table;

  // private final int numThreads;

  public ArrTableCatcher(Workers workers, WorkerData workerData,
    ResourcePool pool, int totalNumPartitions, ArrTable<A, C> table,
    int numThreads) {
    this.workers = workers;
    this.workerData = workerData;
    this.pool = pool;
    this.table = table;
    this.totalNumPartitions = totalNumPartitions;
    // this.numThreads = numThreads;
  }

  public boolean waitAndGet() {
    // If there is only one worker, no allgather
    if (workers.getNumWorkers() == 1) {
      return true;
    }
    // Send the partitions owned by this worker
    int tableID = this.table.getTableID();
    ArrPartition<A>[] ownedPartitions = this.table.getPartitions();
    int numOwnedPartitions = ownedPartitions.length;
    for (int i = 0; i < numOwnedPartitions; i++) {
      deliverArrayPartition(workers, tableID, ownedPartitions[i], pool);
    }
    // Get other partitions from other workers
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
    int partitionID = 0;
    A array = null;
    ArrPartition<A> partition = null;
    ByteArrReqSender byteArraySender = null;
    for (int i = numOwnedPartitions; i < totalNumPartitions; i++) {
      // Wait if data arrives
      data = this.workerData.waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data == null) {
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
      partitionID = metaArray[2];
      try {
        array = ArrParGetter.desiealizeToArray(byteArray, pool,
          table.getAClass());
        partition = new ArrPartition<A>(array, partitionID);
        if (table.addPartition(partition)) {
          RegroupWorker.releaseArrayPartition(partition, this.pool);
        }
      } catch (Exception e) {
        LOG.error("Fail to add partition to table.", e);
        return false;
      }
      // Continue sending to your next neighbor
      if (recvWorkerID != nextWorkerID) {
        byteArraySender = new ByteArrReqSender(nextHost, nextPort, byteArray,
          pool);
        byteArraySender.execute();
      }
      recvByteArrayPartitions.add(byteArray);
    }
    if (!skippedCommData.isEmpty()) {
      workerData.putAllCommData(skippedCommData);
    }
    for (ByteArray byteArr : recvByteArrayPartitions) {
      pool.getByteArrayPool().releaseArrayInUse(byteArr.getArray());
      pool.getIntArrayPool().releaseArrayInUse(byteArr.getMetaArray());
    }
    recvByteArrayPartitions = null;
    return true;
  }

  public ArrTable<A, C> getTable() {
    return this.table;
  }

  private void deliverArrayPartition(Workers workers, int tableID,
    ArrPartition<A> partition, ResourcePool resourcePool) {
    if (partition.getArray().getClass().equals(IntArray.class)) {
      @SuppressWarnings("unchecked")
      ArrPartition<IntArray> intArrPar = (ArrPartition<IntArray>) partition;
      IntArrParDeliver deliver = new IntArrParDeliver(workers, resourcePool,
        tableID, intArrPar);
      deliver.execute();
    } else if (partition.getArray().getClass().equals(DoubleArray.class)) {
      @SuppressWarnings("unchecked")
      ArrPartition<DoubleArray> doubleArrPar = (ArrPartition<DoubleArray>) partition;
      DblArrParDeliver deliver = new DblArrParDeliver(workers, resourcePool,
        tableID, doubleArrPar);
      deliver.execute();
    } else {
      LOG.info("Cannot get correct partition deliver.");
    }
  }
}
