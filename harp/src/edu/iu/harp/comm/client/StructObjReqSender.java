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

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.StructObject;
import edu.iu.harp.comm.resource.DataSerializer;
import edu.iu.harp.comm.resource.ResourcePool;

/**
 * Currently we build the logic based on the simple logic. No fault tolerance is
 * considered.
 * 
 */
public class StructObjReqSender extends ByteArrReqSender {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(StructObjReqSender.class);

  public StructObjReqSender(String host, int port, Commutable data,
    ResourcePool pool) {
    super(host, port, data, pool);
    this.setCommand(Constants.STRUCT_OBJ_REQUEST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    // Convert a struct object to a byte array with meta data
    String className = data.getClass().getName();
    StructObject obj = (StructObject) data;
    int size = obj.getSizeInBytes() + className.length() * 2 + 4;
    // LOG
    //  .info("Struct Obj class name: " + className + ", size in bytes: " + size);
    // Serialize to bytes
    byte[] bytes = this.getResourcePool().getByteArrayPool().getArray(size);
    try {
      serializeStructObjToBytes(className, obj, bytes);
    } catch (Exception e) {
      this.getResourcePool().getByteArrayPool().releaseArrayInUse(bytes);
      throw e;
    }
    // Create byte array
    ByteArray byteArray = new ByteArray();
    byteArray.setArray(bytes);
    byteArray.setSize(size);
    byteArray.setStart(0);
    byteArray.setMetaArray(null);
    byteArray.setMetaArraySize(0);
    return byteArray;
  }

  public static void serializeStructObjToBytes(String className,
    StructObject obj, byte[] bytes) throws Exception {
    DataOutput dataOut = new DataSerializer(bytes);
    dataOut.writeChars(className);
    obj.write(dataOut);
  }
}
