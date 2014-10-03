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
import edu.iu.harp.comm.client.ReqSendCommander;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;


/**
 * When the number of sender threads goes high (e.g. 8),
 * there is some sync issue unsolved yet but the performance 
 * is not seen improved. Don't use this code now.
 * 
 * @author zhangbj
 *
 * @param <P>
 * @param <T>
 */
public class StructParMultiThreadCatcher<P extends StructPartition, T extends StructTable<P>> {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(StructParMultiThreadCatcher.class);

  private final Workers workers;
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final int totalPartitions;
  private final T table;
  private final int numSendThread;
  private final int numDeserialThreads;

  public StructParMultiThreadCatcher(Workers workers, WorkerData workerData,
    ResourcePool pool, int totalPartitions, T table, int numSendThread,
    int numDeserialThreads) {
    this.workers = workers;
    this.workerData = workerData;
    this.pool = pool;
    this.table = table;
    this.totalPartitions = totalPartitions;
    this.numSendThread = numSendThread;
    this.numDeserialThreads = numDeserialThreads;
  }

  public boolean waitAndGet() {
    // Send the partitions owned by this worker
    ReqSendCommander commander = new ReqSendCommander(this.totalPartitions,
      this.numSendThread);
    commander.start();
    P[] ownedPartitions = this.table.getPartitions();
    if (this.workers.getSelfID() != this.workers.getNextID()) {
      for (int i = 0; i < ownedPartitions.length; i++) {
        StructParDeliver<P> deliver = new StructParDeliver<P>(workers, pool,
          ownedPartitions[i]);
        commander.addSender(deliver);
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
      int[] metaArray = byteArray.getMetaArray();
      int workerID = metaArray[0];
      // Continue sending to your next neighbor
      if (workerID != this.workers.getNextID()) {
        ByteArrReqSender byteArraySender = new ByteArrReqSender(workers
          .getNextInfo().getNode(), workers.getNextInfo().getPort(), byteArray,
          pool);
        commander.addSender(byteArraySender);
      }
      recvBinPartitions.add(byteArray);
    }
    // End all sending
    commander.close();
    // If more partitions are received
    LOG.info("recvBinPartitions size: " + recvBinPartitions.size());
    if (recvBinPartitions.size() > 0) {
      List<P> partitions = CollCommWorker.doTasks(recvBinPartitions,
        "allgather-deserialize-executor", new StructParDeserialTask<P>(pool),
        this.numDeserialThreads);
      for (P partition : partitions) {
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
    }
    return true;
  }

  public T getTable() {
    return this.table;
  }
}
