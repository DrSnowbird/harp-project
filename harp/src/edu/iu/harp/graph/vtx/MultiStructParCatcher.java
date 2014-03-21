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
    // Send the partitions owned by this worker in one request
    long time1 = System.currentTimeMillis();
    P[] ownedPartitions = this.table.getPartitions();
    if (this.workers.getSelfID() != this.workers.getNextID()) {
      deliverVtxPartition(workers, new MultiStructPartition<P>(
        new ObjectArrayList<P>(ownedPartitions), pool), pool);
    }
    long time2 = System.currentTimeMillis();
    LOG.info("Send owned partitions: " + (time2 - time1));
    ObjectArrayList<ByteArray> recvBinPartitions = new ObjectArrayList<ByteArray>();
    // Get other partitions from other workers
    for (int i = ownedPartitions.length; i < this.totalPartitions;) {
      long time3 = System.currentTimeMillis();
      // Wait if data arrives
      Commutable data = this.workerData
        .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data == null) {
        return false;
      }
      // Get the byte array, each contains multiple partitions
      ByteArray byteArray = (ByteArray) data;
      int[] metaData = byteArray.getMetaData();
      int workerID = metaData[0];
      int partitionCount = metaData[1];
      // Continue sending to your next neighbor
      if (workerID != this.workers.getNextID()) {
        ByteArrReqSender byteArraySender = new ByteArrReqSender(workers
          .getNextInfo().getNode(), workers.getNextInfo().getPort(), byteArray,
          pool);
        byteArraySender.execute();
      }
      recvBinPartitions.add(byteArray);
      i = i + partitionCount;
      long time4 = System.currentTimeMillis();
      LOG.info("Wait, Get and Send partitions: " + (time4 - time3));
    }
    // If more partitions are received
    LOG.info("recvBinPartitions size: " + recvBinPartitions.size());
    if (recvBinPartitions.size() > 0) {
      long time5 = System.currentTimeMillis();
      List<MultiStructPartition<P>> partitions = CollCommWorker.doTasks(
        recvBinPartitions, "allgather-deserialize-executor",
        new MultiStructParDeserialTask<P>(pool), numThreads);
      long time6 = System.currentTimeMillis();
      for (MultiStructPartition<P> multiPartitions : partitions) {
        for (P partition : multiPartitions.getPartitionList()) {
          try {
            // Fail to add or merge happens
            if (this.table.addPartition(partition) != 1) {
              this.pool.getWritableObjectPool().releaseWritableObjectInUse(
                partition);
            }
          } catch (Exception e) {
            LOG.error("Fail to add partition to table.", e);
            return false;
          }
        }
        // Free multipartition object
        this.pool.getWritableObjectPool().freeWritableObjectInUse(
          multiPartitions);
      }
      long time7 = System.currentTimeMillis();
      LOG.info("Deserialize Data Time: " + (time6 - time5) + " "
        + (time7 - time6));
    }
    return true;
  }

  public T getTable() {
    return this.table;
  }

  private void deliverVtxPartition(Workers workers,
    MultiStructPartition<P> multiPartition, ResourcePool resourcePool) {
    MultiStructParDeliver<P> deliver = new MultiStructParDeliver<P>(workers,
      resourcePool, multiPartition);
    deliver.execute();
  }
}
