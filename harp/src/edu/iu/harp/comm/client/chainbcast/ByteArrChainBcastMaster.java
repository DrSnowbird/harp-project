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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.DataSerializer;
import edu.iu.harp.comm.resource.ResourcePool;

public class ByteArrChainBcastMaster extends ChainBcastMaster {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(ByteArrChainBcastMaster.class);

  public ByteArrChainBcastMaster(Commutable data, Workers workers,
    ResourcePool resourcePool) throws Exception {
    super(data, workers, resourcePool);
    this.setCommand(Constants.BYTE_ARRAY_CHAIN_BCAST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    // Should be byte array
    ByteArray byteArray = (ByteArray) data;
    return byteArray;
  }

  @Override
  protected void sendProcessedData(Connection conn, Commutable data)
    throws Exception {
    // Should be byte array
    ByteArray byteArray = (ByteArray) data;
    sendByteArray(conn, byteArray);
  }

  @Override
  protected void releaseProcessedData(Commutable processedData) {
    // Processed data should be byte array
    if (!(this.getData() instanceof ByteArray)
      && (processedData instanceof ByteArray)) {
      ByteArray byteArray = (ByteArray) processedData;
      this.getResourcePool().getByteArrayPool()
        .releaseArrayInUse(byteArray.getArray());
      if (byteArray.getMetaArray() != null) {
        this.getResourcePool().getIntArrayPool()
          .releaseArrayInUse(byteArray.getMetaArray());
      }
    }
  }

  /**
   * Send command and meta data 1. command 2. byte array size 3. meta data size
   * 4. meta data content.
   * 
   * @param conn
   * @param byteArray
   * @throws Exception
   */
  protected void sendByteArray(Connection conn, ByteArray byteArray)
    throws Exception {
    int size = byteArray.getSize();
    // Get meta array
    int metaArraySize = byteArray.getMetaArraySize();
    // Send meta data
    OutputStream out = conn.getOutputStream();
    out.write(this.getCommand());
    // write byte array size and meta array size
    byte[] sizeBytes = this.getResourcePool().getByteArrayPool().getArray(8);
    DataSerializer serializer = new DataSerializer(sizeBytes);
    serializer.writeInt(size);
    serializer.writeInt(metaArraySize);
    out.write(sizeBytes);
    this.getResourcePool().getByteArrayPool().releaseArrayInUse(sizeBytes);
    // Write meta array
    // array size should match with the array
    // if any inconsistency, use meta array size as standard
    if (metaArraySize > 0) {
      int[] metaArray = byteArray.getMetaArray();
      byte[] metaArrayBytes = this.getResourcePool().getByteArrayPool()
        .getArray(4 * metaArraySize);
      serializer.setData(metaArrayBytes);
      for (int i = 0; i < metaArraySize; i++) {
        serializer.writeInt(metaArray[i]);
      }
      out.write(metaArrayBytes);
      this.getResourcePool().getByteArrayPool()
        .releaseArrayInUse(metaArrayBytes);
    }
    out.flush();
    byte[] bytes = byteArray.getArray();
    int start = byteArray.getStart();
    sendBytes(out, bytes, start, size);
  }

  private void sendBytes(OutputStream out, byte[] bytes, int start, int size)
    throws IOException {
    while ((start + Constants.SENDRECV_BYTE_UNIT) <= size) {
      out.write(bytes, start, Constants.SENDRECV_BYTE_UNIT);
      start = start + Constants.SENDRECV_BYTE_UNIT;
      out.flush();
    }
    if (start < size) {
      out.write(bytes, start, size - start);
      out.flush();
    }
  }
}
