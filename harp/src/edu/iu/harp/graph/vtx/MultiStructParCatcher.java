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

package edu.iu.harp.graph.vtx;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

import org.apache.log4j.Logger;

import edu.iu.harp.collective.CollCommWorker;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.ByteArrReqSender;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.request.MultiStructPartition;
import edu.iu.harp.comm.resource.ResourcePool;

public class MultiStructParCatcher<P extends StructPartition, T extends StructTable<P>> {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(MultiStructParCatcher.class);

  private final Workers workers;
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final int totalPartitions;
  private final T table;
  private final int numThreads;

  public MultiStructParCatcher(Workers workers, WorkerData workerData,
    ResourcePool pool, int totalPartitions, T table, int numThreads) {
    this.workers = workers;
    this.workerData = workerData;
    this.pool = pool;
    this.table = table;
    this.totalPartitions = totalPartitions;
    this.numThreads = numThreads;
  }

  public boolean waitAndGet() {
    // Return success if there is only one worker.
    if (workers.getNumWorkers() <= 1) {
      return true;
    }
    // Send the partitions owned by this worker in one request
    int numWorkers = workers.getNumWorkers();
    int workerID = workers.getSelfID();
    int nextWorkerID = workers.getNextID();
    String nextHost = workers.getNextInfo().getNode();
    int nextPort = workers.getNextInfo().getPort();
    int tableID = table.getTableID();
    P[] ownedPartitions = table.getPartitions();
    int numOwnedPartitions = ownedPartitions.length;
    if (numOwnedPartitions > 0) {
      MultiStructPartition multiStructPartition = (MultiStructPartition) pool
        .getWritableObjectPool().getWritableObject(
          MultiStructPartition.class.getName());
      multiStructPartition.setPartitions(ownedPartitions);
      multiStructPartition.setResourcePool(pool);
      deliverVtxPartition(workerID, nextHost, nextPort, tableID,
        multiStructPartition, pool);
      // Clean and release
      multiStructPartition.clean();
      pool.getWritableObjectPool().releaseWritableObjectInUse(
        multiStructPartition);
    }
    ObjectArrayList<ByteArray> recvBinPartitions = new ObjectArrayList<ByteArray>(
      numWorkers - 1);
    ObjectArrayList<Commutable> skippedCommData = new ObjectArrayList<Commutable>();
    // Get other partitions from other workers
    Commutable data = null;
    ByteArray byteArray = null;
    int[] metaArray = null;
    int recvWorkerID = 0;
    int recvTableID = 0;
    int recvNumPartitions = 0;
    for (int i = numOwnedPartitions; i < totalPartitions;) {
      // Wait if data arrives
      data = this.workerData.waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data == null) {
        return false;
      }
      // Get the byte array, each contains multiple partitions
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
      i += recvNumPartitions;
      // Continue sending to your next neighbor
      if (recvWorkerID != nextWorkerID) {
        ByteArrReqSender byteArraySender = new ByteArrReqSender(nextHost,
          nextPort, byteArray, pool);
        byteArraySender.execute();
      }
      recvBinPartitions.add(byteArray);
    }
    if (!skippedCommData.isEmpty()) {
      workerData.putAllCommData(skippedCommData);
    }
    // If more partitions are received
    // LOG.info("recvBinPartitions size: " + recvBinPartitions.size());
    if (recvBinPartitions.size() > 0) {
      List<MultiStructPartition> partitions = CollCommWorker.doTasks(
        recvBinPartitions, "allgather-deserialize-executor",
        new MultiStructParDeserialTask(pool), numThreads);
      for (MultiStructPartition multiPartitions : partitions) {
        for (StructPartition partition : multiPartitions.getPartitions()) {
          try {
            // !=1 means succeed to add or merge happens
            // try to convert partition to P
            if (this.table.addPartition((P) partition) != 1) {
              this.pool.getWritableObjectPool().releaseWritableObjectInUse(
                partition);
            }
          } catch (Exception e) {
            LOG.error("Fail to add partition to table.", e);
            return false;
          }
        }
        // Release multi-struct-partition object
        multiPartitions.clean();
        pool.getWritableObjectPool()
          .releaseWritableObjectInUse(multiPartitions);
      }
    }
    return true;
  }

  public T getTable() {
    return this.table;
  }

  private void deliverVtxPartition(int workerID, String nextHost, int nextPort,
    int tableID, MultiStructPartition multiPartition, ResourcePool resourcePool) {
    MultiStructParDeliver<P> deliver = new MultiStructParDeliver<P>(workerID,
      nextHost, nextPort, tableID, multiPartition, resourcePool);
    deliver.execute();
  }
}
