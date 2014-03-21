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

import java.io.DataOutputStream;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.WritableObject;
import edu.iu.harp.comm.resource.ResourcePool;

/**
 * Currently we build the logic based on the simple logic. No fault tolerance is
 * considered.
 * 
 */
public class WritableObjReqSenderAlt extends ReqSender {

  public WritableObjReqSenderAlt(String host, int port, Commutable data,
    ResourcePool pool) {
    super(host, port, data, pool);
    this.setCommand(Constants.WRITABLE_OBJ_REQUEST_ALT);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    WritableObject obj = (WritableObject) data;
    return obj;
  }

  @Override
  protected void releaseProcessedData(Commutable data) {
  }

  @Override
  protected void sendProcessedData(Connection conn, Commutable data)
    throws Exception {
    WritableObject obj = (WritableObject) this.getData();
    DataOutputStream dout = conn.getDataOutputStream();
    dout.writeByte(this.getCommand());
    dout.writeUTF(obj.getClass().getName());
    obj.write(dout);
    dout.flush();
  }
}
