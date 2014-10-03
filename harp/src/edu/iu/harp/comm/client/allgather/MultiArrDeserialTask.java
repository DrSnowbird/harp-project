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

import org.apache.log4j.Logger;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.DataDeserializer;
import edu.iu.harp.comm.resource.ResourcePool;

public class MultiArrDeserialTask<A extends Array<?>> extends
  Task<ByteArray, ArrPartition<A>[]> {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(MultiArrDeserialTask.class);

  private final ResourcePool resourcePool;
  private final Class<A> aClass;

  public MultiArrDeserialTask(ResourcePool pool, Class<A> aClass) {
    this.resourcePool = pool;
    this.aClass = aClass;
  }

  @Override
  public ArrPartition<A>[] run(ByteArray byteArray) throws Exception {
    if (!aClass.equals(DoubleArray.class)) {
      throw new Exception("Fail to deserialize multi array partitions");
    }
    ArrPartition<A>[] partitions = null;
    DataDeserializer deserializer = new DataDeserializer(byteArray.getArray());
    // Get the number of partitions from meta array [2]
    int numPartitions = byteArray.getMetaArray()[2];
    partitions = new ArrPartition[numPartitions];
    int partitionID = 0;
    A array = null;
    int arrSize = 0;
    DoubleArray doubleArray = null;
    double[] doubles = null;
    for (int i = 0; i < numPartitions; i++) {
      partitionID = deserializer.readInt();
      arrSize = deserializer.readInt();
      doubles = resourcePool.getDoubleArrayPool().getArray(arrSize);
      for (int j = 0; j < arrSize; j++) {
        doubles[j] = deserializer.readDouble();
      }
      // start should be 0
      doubleArray = new DoubleArray();
      doubleArray.setArray(doubles);
      doubleArray.setSize(arrSize);
      array = (A) doubleArray;
      partitions[i] = new ArrPartition<A>(array, partitionID);
    }
    return partitions;
  }
}
