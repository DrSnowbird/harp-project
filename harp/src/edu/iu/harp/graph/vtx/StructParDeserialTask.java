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

import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.StructObjReqHandler;

public class StructParDeserialTask<P extends StructPartition> extends
  Task<ByteArray, P> {
  private final ResourcePool resourcePool;

  public StructParDeserialTask(ResourcePool pool) {
    this.resourcePool = pool;
  }

  @Override
  public P run(ByteArray byteArray) throws Exception {
    byte[] bytes = byteArray.getArray();
    int[] metaArray = byteArray.getMetaArray();
    if (bytes != null) {
      P partition = (P) StructObjReqHandler.deserializeStructObjFromBytes(
        bytes, byteArray.getSize(), resourcePool);
      if (partition != null) {
        resourcePool.getByteArrayPool().releaseArrayInUse(bytes);
        if (metaArray != null) {
          resourcePool.getIntArrayPool().releaseArrayInUse(
            byteArray.getMetaArray());
        }
        return partition;
      }
    }
    return null;
  }
}
