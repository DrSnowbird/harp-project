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
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.DataSerializer;
import edu.iu.harp.comm.resource.ResourcePool;

public class ArrTableMultiParCatcher<A extends Array<?>, C extends ArrCombiner<A>> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ArrTableMultiParCatcher.class);

  private final Workers workers;
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final int totalNumPartitions;
  private final ArrTable<A, C> table;
  private final int numDeserialThreads;

  public ArrTableMultiParCatcher(Workers workers, WorkerData workerData,
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
    int numOwnedPartitions = ownedPartitions.length;
    // Get worker info
    int numWorkers = workers.getNumWorkers();
    int workerID = workers.getSelfID();
    int nextWorkerID = workers.getNextID();
    String nextHost = workers.getNextInfo().getNode();
    int nextPort = workers.getNextInfo().getPort();
    ByteArray byteArray = null;
    ByteArrReqSender byteArraySender = null;
    if (numOwnedPartitions > 0) {
      try {
        byteArray = serializeMultiArrPartitions(workerID, tableID,
          ownedPartitions, pool);
      } catch (Exception e) {
        LOG.error("Fail to serialize multi array partitions.", e);
        return false;
      }
      byteArraySender = new ByteArrReqSender(nextHost, nextPort, byteArray,
        pool);
      byteArraySender.execute();
      // Release
      pool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
      pool.getIntArrayPool().releaseArrayInUse(byteArray.getMetaArray());
    }
    // You may get numWorkers - 1 arrays at max
    // But some of workers may not send you data
    ObjectArrayList<ByteArray> recvByteArrayPartitions = new ObjectArrayList<ByteArray>(
      numWorkers - 1);
    ObjectArrayList<Commutable> skippedCommData = new ObjectArrayList<Commutable>();
    Commutable data = null;
    int[] metaArray = null;
    int recvWorkerID = 0;
    int recvTableID = 0;
    int recvNumPartitions = 0;
    for (int i = numOwnedPartitions; i < totalNumPartitions;) {
      // Wait if data arrives
      data = workerData.waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data == null) {
        return false;
      }
      // Skip unnecessary data
      if (!(data instanceof ByteArray)) {
        skippedCommData.add(data);
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
        continue;
      }
      // Some collective communication on table only has worker and table info
      recvNumPartitions = metaArray[2];
      // If this array is what we want to receive...
      i += recvNumPartitions;
      // Continue sending to your next neighbor
      if (recvWorkerID != nextWorkerID) {
        byteArraySender = new ByteArrReqSender(nextHost, nextPort, byteArray,
          pool);
        // ReqSendCenter.submit(byteArraySender);
        // Send without using another thread
        byteArraySender.execute();
      }
      recvByteArrayPartitions.add(byteArray);
    }
    if (!skippedCommData.isEmpty()) {
      workerData.putAllCommData(skippedCommData);
    }
    // Deserialize
    List<ArrPartition<A>[]> multiPartitions = CollCommWorker.doTasks(
      recvByteArrayPartitions, "allgather-deserialize-executor",
      new MultiArrDeserialTask<A>(pool, table.getAClass()), numDeserialThreads);
    for (ArrPartition<A>[] multiPartition : multiPartitions) {
      for (ArrPartition<A> partition : multiPartition) {
        try {
          if (table.addPartition(partition)) {
            RegroupWorker.releaseArrayPartition(partition, this.pool);
          }
        } catch (Exception e) {
          LOG.error("Fail to add partition to table.", e);
          return false;
        }
      }
    }
    // Must finish sending, otherwise data may be released but not sent
    // ReqSendCenter.waitForFinish();
    // Release
    for (ByteArray byteArr : recvByteArrayPartitions) {
      pool.getByteArrayPool().releaseArrayInUse(byteArr.getArray());
      pool.getIntArrayPool().releaseArrayInUse(byteArr.getMetaArray());
    }
    recvByteArrayPartitions = null;
    return true;
  }

  public ArrTable<A, C> getTable() {
    return table;
  }

  static <A extends Array<?>> ByteArray serializeMultiArrPartitions(
    int workerID, int tableID, ArrPartition<A>[] partitions, ResourcePool pool)
    throws Exception {
    int size = 0;
    byte[] bytes = null;
    int numPartitions = partitions.length;
    if (numPartitions > 0) {
      if (partitions[0].getArray().getClass().equals(DoubleArray.class)) {
        @SuppressWarnings("unchecked")
        ArrPartition<DoubleArray>[] dblArrPartitions = (ArrPartition<DoubleArray>[]) partitions;
        size = 0;
        for (ArrPartition<DoubleArray> dblArrPartition : dblArrPartitions) {
          // partition ID and array size, each 4 bytes, then each double is 8
          // bytes
          size = size + 8 + dblArrPartition.getArray().getSize() * 8;
        }
        // Use resource pool to get a large byte array
        bytes = pool.getByteArrayPool().getArray(size);
        double[] doubles = null;
        int arrSize = 0;
        DataSerializer serializer = new DataSerializer(bytes);
        // Write number of partitions
        // serializer.writeInt(dblArrPartitions.length);
        try {
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
        } catch (Exception e) {
          pool.getByteArrayPool().releaseArrayInUse(bytes);
          throw new Exception("Fail to serialize partitions.");
        }
      } else {
        throw new Exception("Cannot serialize partitions.");
      }
    }
    ByteArray byteArray = new ByteArray();
    byteArray.setArray(bytes);
    int[] metaArray = pool.getIntArrayPool().getArray(3);
    metaArray[0] = workerID;
    metaArray[1] = tableID;
    metaArray[2] = numPartitions;
    byteArray.setMetaArray(metaArray);
    byteArray.setMetaArraySize(3);
    byteArray.setSize(size);
    byteArray.setStart(0);
    return byteArray;
  }
}
