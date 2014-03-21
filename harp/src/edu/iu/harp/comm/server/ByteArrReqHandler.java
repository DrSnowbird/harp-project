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

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;

public class ByteArrReqHandler extends ReqHandler {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ByteArrReqHandler.class);

  public ByteArrReqHandler(WorkerData workerData, ResourcePool pool,
    Connection conn) {
    super(workerData, pool, conn);
  }

  @Override
  protected Commutable handleData(Connection conn) throws Exception {
    DataInputStream din = conn.getDataInputDtream();
    // Receive data
    ByteArray byteArray = receiveByteArray(din);
    // Close connection
    conn.close();
    // Process byte array
    Commutable data = null;
    try {
      data = processByteArray(byteArray);
    } catch (Exception e) {
      releaseBytes(byteArray.getArray());
      throw e;
    }
    return data;
  }

  private ByteArray receiveByteArray(DataInputStream din) throws IOException {
    int size = din.readInt();
    int metaDataSize = din.readInt();
    int[] metaData = null;
    if (metaDataSize > 0) {
      // Meta data should be small, not managed by resource pool
      metaData = new int[metaDataSize];
      for (int i = 0; i < metaDataSize; i++) {
        metaData[i] = din.readInt();
      }
    }
    // Prepare bytes from resource pool
    byte[] bytes = this.getResourcePool().getByteArrayPool()
      .getArray(size + Constants.SENDRECV_BYTE_UNIT);
    try {
      receiveBytes(din, bytes, size);
    } catch (Exception e) {
      releaseBytes(bytes);
      throw e;
    }
    ByteArray array = new ByteArray();
    array.setArray(bytes);
    array.setStart(0);
    array.setSize(size);
    array.setMetaData(metaData);
    LOG.info("Byte array size: " + size + ", real size: " + bytes.length
      + ", meta data size: " + metaDataSize);
    return array;
  }

  private void receiveBytes(DataInputStream din, byte[] bytes, int size)
    throws IOException {
    // Receive bytes data and process
    int recvLen = 0;
    int len = 0;
    while (recvLen < size) {
      len = din.read(bytes, recvLen, Constants.SENDRECV_BYTE_UNIT);
      recvLen = recvLen + len;
      if (recvLen == size) {
        break;
      }
    }
  }

  private void releaseBytes(byte[] bytes) {
    this.getResourcePool().getByteArrayPool().releaseArrayInUse(bytes);
  }

  protected Commutable processByteArray(ByteArray array) throws Exception {
    return array;
  }
}
