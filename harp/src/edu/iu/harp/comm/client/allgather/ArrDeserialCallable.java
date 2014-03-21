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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.comm.client.regroup.ArrParGetter;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.resource.ResourcePool;

class Result<A extends Array<?>> {

  private ObjectArrayList<ArrPartition<A>> partitions;

  public Result() {
    this.partitions = new ObjectArrayList<ArrPartition<A>>();
  }

  public void addArrPartition(ArrPartition<A> partition) {
    this.partitions.add(partition);
  }

  public ObjectArrayList<ArrPartition<A>> getArrPartitions() {
    return this.partitions;
  }
}
  
public class ArrDeserialCallable<A extends Array<?>> implements
  Callable<Result<A>> {

  private final ResourcePool resourcePool;
  private final BlockingQueue<ByteArray> partitionQueue;
  private final Class<A> aClass;

  public ArrDeserialCallable(ResourcePool pool, BlockingQueue<ByteArray> queue,
    Class<A> aClass) {
    this.resourcePool = pool;
    this.partitionQueue = queue;
    this.aClass = aClass;
  }

  @Override
  public Result<A> call() throws Exception {
    Result<A> result = new Result<A>();
    while (!partitionQueue.isEmpty()) {
      ByteArray byteArray = partitionQueue.poll();
      if (byteArray == null) {
        break;
      }
      int partitionID = byteArray.getMetaData()[1];
      A array = ArrParGetter.desiealizeToArray(byteArray, resourcePool, aClass);
      if (array != null) {
        ArrPartition<A> partition = new ArrPartition<A>(array, partitionID);
        result.addArrPartition(partition);
        // Release byte array
        this.resourcePool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
      }
    }
    return result;
  }
}
