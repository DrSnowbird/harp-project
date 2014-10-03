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

package edu.iu.harp.comm.client.allbcast;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

import org.apache.log4j.Logger;

import edu.iu.harp.collective.CollCommWorker;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.graph.vtx.StructParDeserialTask;
import edu.iu.harp.graph.vtx.StructPartition;
import edu.iu.harp.graph.vtx.StructTable;

public class StructTableBcastSlave<P extends StructPartition, T extends StructTable<P>> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(StructTableBcastSlave.class);

  private final Workers workers;
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final int totalPartitions;
  private final T table;
  private final int numThreads;

  public StructTableBcastSlave(Workers workers, WorkerData workerData,
    ResourcePool pool, int totalPartitions, T table, int numThreads) {
    this.workers = workers;
    this.workerData = workerData;
    this.pool = pool;
    this.table = table;
    this.totalPartitions = totalPartitions;
    this.numThreads = numThreads;
  }

  public boolean waitAndGet() {
    if (this.workers.isMaster()) {
      return true;
    }
    ObjectArrayList<ByteArray> recvBinPartitions = new ObjectArrayList<ByteArray>();
    ObjectArrayList<Commutable> skippedCommData = new ObjectArrayList<Commutable>();
    // Get other partitions from other workers
    for (int i = 0; i < this.totalPartitions; i++) {
      // Wait if data arrives
      Commutable data = this.workerData
        .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
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
      ByteArray byteArray = (ByteArray) data;
      recvBinPartitions.add(byteArray);
    }
    // If more partitions are received
    LOG.info("recvBinPartitions size: " + recvBinPartitions.size());
    if (recvBinPartitions.size() > 0) {
      // Create queue to deserialize byte arrays
      List<P> partitions = CollCommWorker.doTasks(recvBinPartitions,
        "allbcast-deserialize-executor", new StructParDeserialTask<P>(pool),
        numThreads);
      for (P partition : partitions) {
        try {
          // If fail to add or merge happens
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
