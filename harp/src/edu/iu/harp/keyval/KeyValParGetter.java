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

package edu.iu.harp.keyval;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.StructObjReqHandler;

public class KeyValParGetter<K extends Key, V extends Value, C extends ValCombiner<V>> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(KeyValParGetter.class);

  private final WorkerData workerData;
  private final ResourcePool resourcePool;

  public KeyValParGetter(WorkerData workerData, ResourcePool pool) {
    this.workerData = workerData;
    this.resourcePool = pool;
  }

  public KeyValPartition<K, V, C> waitAndGet(long timeOut) {
    // Wait if data arrives
    Commutable data = this.workerData.waitAndGetCommData(timeOut);
    if (data == null) {
      return null;
    }
    ByteArray byteArray = (ByteArray) data;
    KeyValPartition<K, V, C> partition = getKeyValPartition(byteArray);
    if (partition == null) {
      return null;
    }
    return partition;
  }

  @SuppressWarnings("unchecked")
  private KeyValPartition<K, V, C> getKeyValPartition(ByteArray byteArray) {
    KeyValPartition<K, V, C> partition = null;
    try {
      partition = (KeyValPartition<K, V, C>) StructObjReqHandler
        .deserializeStructObjFromBytes(byteArray.getArray(),
          byteArray.getSize(), this.resourcePool);
    } catch (Exception e) {
      LOG.error("Fail to create KeyVal partition object.", e);
      return null;
    }
    partition.setPartitionID(byteArray.getMetaArray()[0]);
    this.resourcePool.getByteArrayPool()
      .releaseArrayInUse(byteArray.getArray());
    this.resourcePool.getIntArrayPool().releaseArrayInUse(
      byteArray.getMetaArray());
    return partition;
  }
}
