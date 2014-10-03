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

package edu.iu.harp.comm.server;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.resource.DataDeserializer;
import edu.iu.harp.comm.resource.ResourcePool;

public class IntArrReqHandler extends ByteArrReqHandler {

  public IntArrReqHandler(WorkerData workerData, ResourcePool pool,
    Connection conn) {
    super(workerData, pool, conn);
  }

  @Override
  protected Commutable processByteArray(ByteArray byteArray) throws Exception {
    IntArray intArray = deserializeBytesToIntArray(byteArray.getArray(),
      this.getResourcePool());
    // If success, release bytes
    // Meta array doesn't exist in sending int array
    this.getResourcePool().getByteArrayPool()
      .releaseArrayInUse(byteArray.getArray());
    return intArray;
  }

  public static IntArray deserializeBytesToIntArray(byte[] bytes,
    ResourcePool resourcePool) throws Exception {
    int[] ints = null;
    int intsSize = 0;
    DataDeserializer din = new DataDeserializer(bytes);
    try {
      intsSize = din.readInt();
      ints = resourcePool.getIntArrayPool().getArray(intsSize);
      for (int i = 0; i < intsSize; i++) {
        ints[i] = din.readInt();
      }
    } catch (Exception e) {
      if (ints != null) {
        resourcePool.getIntArrayPool().releaseArrayInUse(ints);
      }
      throw e;
    }
    IntArray intArray = new IntArray();
    intArray.setArray(ints);
    intArray.setSize(intsSize);
    return intArray;
  }
}
