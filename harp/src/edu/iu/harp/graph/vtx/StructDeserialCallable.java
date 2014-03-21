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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.StructObjReqHandler;

class Result<P extends StructPartition> {

  private ObjectArrayList<P> partitions;

  public Result() {
    this.partitions = new ObjectArrayList<P>();
  }

  public void addPartition(P partition) {
    this.partitions.add(partition);
  }

  public ObjectArrayList<P> getPartitions() {
    return this.partitions;
  }
}

public class StructDeserialCallable<P extends StructPartition> implements
  Callable<Result<P>> {

  private final ResourcePool resourcePool;
  private final BlockingQueue<ByteArray> partitionQueue;

  public StructDeserialCallable(ResourcePool pool, BlockingQueue<ByteArray> queue) {
    this.resourcePool = pool;
    this.partitionQueue = queue;
  }

  @Override
  public Result<P> call() throws Exception {
    Result<P> result = new Result<P>();
    while (!partitionQueue.isEmpty()) {
      ByteArray byteArray = partitionQueue.poll();
      if (byteArray == null) {
        break;
      }
      P partition = (P) StructObjReqHandler.deserializeStructObjFromBytes(
        byteArray.getArray(), byteArray.getSize(), resourcePool);
      if (partition != null) {
        result.addPartition(partition);
        // Release byte array
        this.resourcePool.getByteArrayPool().releaseArrayInUse(
          byteArray.getArray());
      }
    }
    return result;
  }
}
