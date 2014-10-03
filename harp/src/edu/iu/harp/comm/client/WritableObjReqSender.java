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
import java.io.OutputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.data.ByteArrayStream;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.WritableObject;
import edu.iu.harp.comm.resource.DataSerializer;
import edu.iu.harp.comm.resource.ResourcePool;

/**
 * This writable object request sender uses ByteArrayOutputStream. Because all
 * objects we use now are known in size. This class can be considered as
 * deprecated.
 */
public class WritableObjReqSender extends ReqSender {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(WritableObjReqSender.class);

  public WritableObjReqSender(String host, int port, Commutable data,
    ResourcePool pool) {
    super(host, port, data, pool);
    this.setCommand(Constants.WRITABLE_OBJ_REQUEST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    // Should be WritableObject
    WritableObject obj = (WritableObject) this.getData();
    String className = obj.getClass().getName();
    LOG.info("Class name: " + obj.getClass().getName());
    ByteArrayOutputStream byteOut = this.getResourcePool()
      .getByteArrayOutputStreamPool().getByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOut);
    try {
      dataOut.writeUTF(className);
      obj.write(dataOut);
      dataOut.flush();
    } catch (Exception e) {
      this.getResourcePool().getByteArrayOutputStreamPool()
        .releaseByteArrayOutputStreamInUse(byteOut);
      throw e;
    }
    LOG.info("Serialized writable object size: " + byteOut.size());
    ByteArrayStream stream = new ByteArrayStream();
    stream.setByteArrayStream(byteOut);
    return stream;
  }

  @Override
  protected void releaseProcessedData(Commutable data) {
    ByteArrayStream stream = (ByteArrayStream) data;
    ByteArrayOutputStream byteOut = stream.getByteArrayStream();
    this.getResourcePool().getByteArrayOutputStreamPool()
      .releaseByteArrayOutputStreamInUse(byteOut);
  }

  @Override
  protected void sendProcessedData(Connection conn, Commutable data)
    throws Exception {
    // This is proved to be inefficient
    // So the implementation is not well abstracted
    ByteArrayStream stream = (ByteArrayStream) data;
    ByteArrayOutputStream byteOut = stream.getByteArrayStream();
    int size = byteOut.size();
    OutputStream out = conn.getOutputStream();
    out.write(this.getCommand());
    byte[] sizeBytes = this.getResourcePool().getByteArrayPool().getArray(8);
    DataSerializer serializer = new DataSerializer(sizeBytes);
    serializer.writeInt(size);
    serializer.writeInt(0); // No meta data
    out.write(sizeBytes);
    this.getResourcePool().getByteArrayPool().releaseArrayInUse(sizeBytes);
    out.flush();
    byteOut.writeTo(out);
    out.flush();
  }
}
