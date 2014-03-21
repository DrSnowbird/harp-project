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

import java.io.DataInput;

import org.apache.log4j.Logger;

import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.request.MultiStructPartition;
import edu.iu.harp.comm.resource.DataDeserializer;
import edu.iu.harp.comm.resource.ResourcePool;

public class MultiStructParDeserialTask<P extends StructPartition> extends
  Task<ByteArray, MultiStructPartition<P>> {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(MultiStructParDeserialTask.class);

  private final ResourcePool resourcePool;

  public MultiStructParDeserialTask(ResourcePool pool) {
    this.resourcePool = pool;
  }

  @Override
  public MultiStructPartition<P> run(ByteArray byteArray) throws Exception {
    MultiStructPartition<P> multiPartitions = null;
    DataInput din = new DataDeserializer(byteArray.getArray());
    try {
      String className = din.readUTF();
      multiPartitions = (MultiStructPartition<P>) resourcePool
        .getWritableObjectPool().getWritableObject(className);
      multiPartitions.setResourcePool(this.resourcePool);
      multiPartitions.read(din);
      LOG.info("Class name: " + className + ".");
    } catch (Exception e) {
      LOG.error("Error in deserialization...", e);
      // Free if error
      if (multiPartitions != null) {
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(
          multiPartitions);
      }
      throw e;
    }
    if (multiPartitions != null) {
      resourcePool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
    }
    return multiPartitions;
  }
}
