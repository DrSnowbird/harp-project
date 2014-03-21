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

import edu.iu.harp.arrpar.ArrCombiner;
import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.collective.CollCommWorker;
import edu.iu.harp.collective.RegroupWorker;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.allbcast.ArrDeserialTask;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;

public class ArrTableBcastSlave<A extends Array<?>, C extends ArrCombiner<A>> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ArrTableBcastSlave.class);

  private final Workers workers;
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final int totalPartitions;
  private final ArrTable<A, C> table;
  private final int numThreads;

  public ArrTableBcastSlave(Workers workers, WorkerData workerData,
    ResourcePool pool, int totalPartitions, ArrTable<A, C> table, int numThreads) {
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
    // Get other partitions from other workers
    for (int i = 0; i < this.totalPartitions; i++) {
      // Wait if data arrives
      Commutable data = this.workerData
        .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data == null) {
        return false;
      }
      // Get the byte array
      ByteArray byteArray = (ByteArray) data;
      recvBinPartitions.add(byteArray);
    }
    // If more partitions are received
    LOG.info("recvBinPartitions size: " + recvBinPartitions.size());
    if (recvBinPartitions.size() > 0) {
      // Create queue to deserialize byte arrays
      long start = System.currentTimeMillis();
      List<ArrPartition<A>> partitions = CollCommWorker.doTasks(
        recvBinPartitions, "allbcast-deserialize-executor",
        new ArrDeserialTask<A>(pool, table.getAClass()), numThreads);
      long end = System.currentTimeMillis();
      LOG.info("Deserialization Time (ms): " + (end - start));
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
      long end2 = System.currentTimeMillis();
      LOG.info("Add partition Time (ms): " + (end2 - start));
    }
    return true;
  }

  public ArrTable<A, C> getTable() {
    return this.table;
  }
}
