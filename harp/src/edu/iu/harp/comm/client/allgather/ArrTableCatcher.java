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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

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
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class ArrTableCatcher<A extends Array<?>, C extends ArrCombiner<A>> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ArrTableCatcher.class);

  private final Workers workers;
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final int totalPartitions;
  private final ArrTable<A, C> table;
  private final int numThreads;

  public ArrTableCatcher(Workers workers, WorkerData workerData, ResourcePool pool,
    int totalPartitions, ArrTable<A, C> table, int numThreads) {
    this.workers = workers;
    this.workerData = workerData;
    this.pool = pool;
    this.table = table;
    this.totalPartitions = totalPartitions;
    this.numThreads = numThreads;
  }

  public boolean waitAndGet() {
    // Send the partitions owned by this worker
    ArrPartition<A>[] ownedPartitions = this.table.getPartitions();
    if (this.workers.getSelfID() != this.workers.getNextID()) {
      for (int i = 0; i < ownedPartitions.length; i++) {
        deliverArrayPartition(workers, ownedPartitions[i], pool);
      }
    }
    ObjectArrayList<ByteArray> recvBinPartitions = new ObjectArrayList<ByteArray>();
    // Get other partitions from other workers
    for (int i = ownedPartitions.length; i < this.totalPartitions; i++) {
      // Wait if data arrives
      Commutable data = this.workerData
        .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data == null) {
        return false;
      }
      // Get the byte array
      ByteArray byteArray = (ByteArray) data;
      int[] metaData = byteArray.getMetaData();
      int workerID = metaData[0];
      // Continue sending to your next neighbor
      if (workerID != this.workers.getNextID()) {
        ByteArrReqSender byteArraySender = new ByteArrReqSender(workers
          .getNextInfo().getNode(), workers.getNextInfo().getPort(), byteArray,
          pool);
        byteArraySender.execute();
      }
      recvBinPartitions.add(byteArray);
    }
    // If more partitions are received
    LOG.info("recvBinPartitions size: " + recvBinPartitions.size());
    if (recvBinPartitions.size() > 0) {
      // Create queue to deserialize byte arrays
      /*
      final BlockingQueue<ByteArray> partitionQueue = new ArrayBlockingQueue<ByteArray>(
        recvBinPartitions.size());
      for (ByteArray array : recvBinPartitions) {
        partitionQueue.add(array);
      }
      List<Callable<Result<A>>> tasks = new ArrayList<Callable<Result<A>>>();
      for (int i = 0; i < numThreads; i++) {
        tasks.add(new ArrDeserialCallable<A>(pool, partitionQueue, table
          .getAClass()));
      }
      Set<Result<A>> results = CollCommWorker.doTasks(tasks,
        "allgather-deserialize-executor");
      ObjectArrayList<ArrPartition<A>> partitions = new ObjectArrayList<ArrPartition<A>>();
      for (Result<A> result : results) {
        partitions.addAll(result.getArrPartitions());
      }
      */

      List<ArrPartition<A>> partitions = CollCommWorker.doTasks(
        recvBinPartitions, "allgather-deserialize-executor",
        new ArrDeserialTask<A>(pool, table.getAClass()), numThreads);
 
      for (ArrPartition<A> partition : partitions) {
        try {
          if (this.table.addPartition(partition)) {
            RegroupWorker.releaseArrayPartition(partition, this.pool);
          }
        } catch (Exception e) {
          LOG.error("Fail to add partition to table.", e);
          return false;
        }
      }
    }
    return true;
  }

  public ArrTable<A, C> getTable() {
    return this.table;
  }

  private void deliverArrayPartition(Workers workers,
    ArrPartition<A> partition, ResourcePool resourcePool) {
    if (partition.getArray().getClass().equals(IntArray.class)) {
      @SuppressWarnings("unchecked")
      ArrPartition<IntArray> intArrPar = (ArrPartition<IntArray>) partition;
      IntArrParDeliver deliver = new IntArrParDeliver(workers, resourcePool,
        intArrPar);
      deliver.execute();
    } else if (partition.getArray().getClass().equals(DoubleArray.class)) {
      @SuppressWarnings("unchecked")
      ArrPartition<DoubleArray> doubleArrPar = (ArrPartition<DoubleArray>) partition;
      DblArrParDeliver deliver = new DblArrParDeliver(workers,
        resourcePool, doubleArrPar);
      deliver.execute();
    } else {
      LOG.info("Cannot get correct partition deliver.");
    }
  }
}
