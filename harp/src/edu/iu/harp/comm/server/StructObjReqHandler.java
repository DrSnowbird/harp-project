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

import java.io.DataInput;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.StructObject;
import edu.iu.harp.comm.resource.DataDeserializer;
import edu.iu.harp.comm.resource.ResourcePool;

public class StructObjReqHandler extends ByteArrReqHandler {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(StructObjReqHandler.class);

  public StructObjReqHandler(WorkerData workerData, ResourcePool pool,
    Connection conn) {
    super(workerData, pool, conn);
  }

  protected Commutable processByteArray(ByteArray byteArray) throws Exception {
    byte[] bytes = byteArray.getArray();
    if (bytes != null) {
      StructObject obj = deserializeStructObjFromBytes(bytes,
        byteArray.getSize(), this.getResourcePool());
      // If success, release bytes
      // Meta array doesn't exist in sending struct object
      this.getResourcePool().getByteArrayPool().releaseArrayInUse(bytes);
      return obj;
    } else {
      return null;
    }
  }

  public static StructObject deserializeStructObjFromBytes(byte[] bytes,
    int size, ResourcePool resourcePool) throws Exception {
    if (bytes == null || size == 0) {
      return null;
    }
    StructObject obj = null;
    DataInput din = new DataDeserializer(bytes);
    try {
      String className = din.readUTF();
      obj = (StructObject) resourcePool.getWritableObjectPool()
        .getWritableObject(className);
      obj.read(din);
      // LOG.info("Class name: " + className + ", size: " +
      // obj.getSizeInBytes());
    } catch (Exception e) {
      LOG.error("Error in deserialization...", e);
      // Free if error
      if (obj != null) {
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(obj);
      }
      throw e;
    }
    return obj;
  }
}
