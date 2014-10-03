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
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.DataDeserializer;
import edu.iu.harp.comm.resource.ResourcePool;

public class DoubleArrReqHandler extends ByteArrReqHandler {

  public DoubleArrReqHandler(WorkerData workerData, ResourcePool pool,
    Connection conn) {
    super(workerData, pool, conn);
  }

  @Override
  protected Commutable processByteArray(ByteArray byteArray) throws Exception {
    byte[] bytes = byteArray.getArray();
    int size = byteArray.getSize();
    DoubleArray doubleArray = null;
    if (bytes != null && size > 0) {
      doubleArray = deserializeBytesToDoubleArray(byteArray.getArray(),
        this.getResourcePool());
      // If success, release bytes
      // Meta array doesn't exist in sending double array
      this.getResourcePool().getByteArrayPool()
        .releaseArrayInUse(byteArray.getArray());
    } else {
      doubleArray = new DoubleArray();
      doubleArray.setArray(null);
      doubleArray.setSize(0);
    }
    return doubleArray;
  }

  public static DoubleArray deserializeBytesToDoubleArray(byte[] bytes,
    ResourcePool resourcePool) throws Exception {
    double[] doubles = null;
    int doublesSize = 0;
    DataDeserializer din = new DataDeserializer(bytes);
    try {
      doublesSize = din.readInt();
      doubles = resourcePool.getDoubleArrayPool().getArray(doublesSize);
      for (int i = 0; i < doublesSize; i++) {
        doubles[i] = din.readDouble();
      }
    } catch (Exception e) {
      if (doubles != null) {
        resourcePool.getDoubleArrayPool().releaseArrayInUse(doubles);
      }
      throw e;
    }
    DoubleArray doubleArray = new DoubleArray();
    // Start is 0 in default
    doubleArray.setArray(doubles);
    doubleArray.setSize(doublesSize);
    return doubleArray;
  }
}
