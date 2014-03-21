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
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.WritableObject;
import edu.iu.harp.comm.resource.ResourcePool;

public class WritableObjReqHandlerAlt extends ReqHandler {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(WritableObjReqHandlerAlt.class);

  public WritableObjReqHandlerAlt(WorkerData workerData, ResourcePool pool,
    Connection conn) {
    super(workerData, pool, conn);
  }

  @Override
  protected Commutable handleData(Connection conn) throws Exception {
    DataInput din = conn.getDataInputDtream();
    String className = din.readUTF();
    LOG.info("Class name: " + className);
    WritableObject obj = this.getResourcePool().getWritableObjectPool()
      .getWritableObject(className);
    try {
      obj.read(din);
    } catch (Exception e) {
      // Make the object be accessible next time
      this.getResourcePool().getWritableObjectPool()
        .releaseWritableObjectInUse(obj);
      throw e;
    }
    // Close connection
    conn.close();
    return obj;
  }
}
