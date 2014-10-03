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

package edu.iu.harp.comm.client;

import java.io.DataOutput;

import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.resource.DataSerializer;
import edu.iu.harp.comm.resource.ResourcePool;

public class IntArrReqSender extends ByteArrReqSender {

  public IntArrReqSender(String host, int port, Commutable data,
    ResourcePool pool) {
    super(host, port, data, pool);
    this.setCommand(Constants.INT_ARRAY_REQUEST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    // Convert an int array to a byte array with meta data
    IntArray intArray = (IntArray) this.getData();
    int[] ints = intArray.getArray();
    int intsSize = intArray.getSize();
    int size = intsSize * 4 + 4;
    byte[] bytes = this.getResourcePool().getByteArrayPool().getArray(size);
    try {
      serializeIntsToBytes(ints, intsSize, bytes);
    } catch (Exception e) {
      this.getResourcePool().getByteArrayPool().releaseArrayInUse(bytes);
      throw e;
    }
    ByteArray byteArray = new ByteArray();
    byteArray.setArray(bytes);
    byteArray.setMetaArray(null);
    byteArray.setMetaArraySize(0);
    byteArray.setSize(size);
    byteArray.setStart(0);
    return byteArray;
  }

  public static void serializeIntsToBytes(int[] ints, int intsSize,
    byte[] bytes) throws Exception {
    DataOutput dataOut = new DataSerializer(bytes);
    dataOut.writeInt(intsSize);
    for (int i = 0; i < intsSize; i++) {
      dataOut.writeInt(ints[i]);
    }
  }
}
