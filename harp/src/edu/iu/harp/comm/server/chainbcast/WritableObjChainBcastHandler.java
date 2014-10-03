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

package edu.iu.harp.comm.server.chainbcast;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.WritableObject;
import edu.iu.harp.comm.resource.ResourcePool;

public class WritableObjChainBcastHandler extends ByteArrChainBcastHandler {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(WritableObjChainBcastHandler.class);

  public WritableObjChainBcastHandler(WorkerData workerData, ResourcePool pool,
    Connection conn, Workers workers, byte command) {
    super(workerData, pool, conn, workers, command);
  }

  @Override
  protected Commutable processByteArray(ByteArray byteArray) throws Exception {
    WritableObject obj = null;
    DataInputStream din = new DataInputStream(new ByteArrayInputStream(
      byteArray.getArray()));
    try {
      String className = din.readUTF();
      LOG.info("Class name: " + className);
      obj = this.getResourcePool().getWritableObjectPool()
        .getWritableObject(className);
      obj.read(din);
    } catch (Exception e) {
      if (obj != null) {
        this.getResourcePool().getWritableObjectPool()
          .releaseWritableObjectInUse(obj);
      }
      throw e;
    }
    // Meta array doesn't exist in broadcasting writable object
    this.getResourcePool().getByteArrayPool()
      .releaseArrayInUse(byteArray.getArray());
    return obj;
  }
}
