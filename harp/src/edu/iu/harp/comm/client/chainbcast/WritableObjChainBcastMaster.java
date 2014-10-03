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

package edu.iu.harp.comm.client.chainbcast;

import java.io.DataOutputStream;
import java.io.OutputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.data.ByteArrayStream;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.WritableObject;
import edu.iu.harp.comm.resource.DataSerializer;
import edu.iu.harp.comm.resource.ResourcePool;

public class WritableObjChainBcastMaster extends ChainBcastMaster {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(WritableObjChainBcastMaster.class);

  public WritableObjChainBcastMaster(Commutable data, Workers workers,
    ResourcePool pool) throws Exception {
    super(data, workers, pool);
    this.setCommand(Constants.WRITABLE_OBJ_CHAIN_BCAST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    // Should be WritableObject
    WritableObject obj = (WritableObject) this.getData();
    String className = obj.getClass().getName();
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
    LOG.info("Class name: " + obj.getClass().getName()
      + ", Serialized writable object size: " + byteOut.size());
    ByteArrayStream stream = new ByteArrayStream();
    stream.setByteArrayStream(byteOut);
    return stream;
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
    try {
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
    } finally {
      this.getResourcePool().getByteArrayOutputStreamPool()
        .releaseByteArrayOutputStreamInUse(byteOut);
    }
  }

  @Override
  protected void releaseProcessedData(Commutable data) {
    ByteArrayStream stream = (ByteArrayStream) data;
    ByteArrayOutputStream byteOut = stream.getByteArrayStream();
    this.getResourcePool().getByteArrayOutputStreamPool()
      .releaseByteArrayOutputStreamInUse(byteOut);
  }
}
