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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;

public class ByteArrChainBcastHandler extends ChainBcastHandler {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(ByteArrChainBcastHandler.class);

  public ByteArrChainBcastHandler(WorkerData workerData, ResourcePool pool,
    Connection conn, Workers workers, byte command) {
    super(workerData, pool, conn, workers, command);
  }

  protected Commutable receiveData(Connection conn, Connection nextConn)
    throws Exception {
    DataInputStream din = conn.getDataInputDtream();
    DataOutputStream doutNextWorker = null;
    if (nextConn != null) {
      doutNextWorker = nextConn.getDataOutputStream();
    }
    // Receive data
    ByteArray byteArray = receiveByteArray(din, doutNextWorker);
    // Close connection
    conn.close();
    if (nextConn != null) {
      nextConn.close();
    }
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

  /**
   * Receive 1. command 2. byte array size 3. meta data size 4. meta data
   * content
   * 
   * @param din
   * @param doutNextWorker
   * @return
   * @throws Exception
   */
  private ByteArray receiveByteArray(DataInputStream din,
    DataOutputStream doutNextWorker) throws Exception {
    int size = din.readInt();
    int metaDataSize = din.readInt();
    int[] metaData = null;
    if (doutNextWorker != null) {
      doutNextWorker.writeByte(this.getCommand());
      doutNextWorker.writeInt(size);
      doutNextWorker.writeInt(metaDataSize);
      doutNextWorker.flush();
    }
    if (metaDataSize > 0) {
      // LOG.info("Meta data size: " + metaDataSize);
      metaData = new int[metaDataSize];
      for (int i = 0; i < metaDataSize; i++) {
        metaData[i] = din.readInt();
      }
      if (doutNextWorker != null) {
        for (int i = 0; i < metaDataSize; i++) {
          doutNextWorker.writeInt(metaData[i]);
        }
        doutNextWorker.flush();
      }
    }
    // Prepare internal bytes structure
    byte[] bytes = this.getResourcePool().getByteArrayPool()
      .getArray(size + Constants.SENDRECV_BYTE_UNIT);
    try {
      receiveBytes(din, doutNextWorker, bytes, size);
    } catch (Exception e) {
      releaseBytes(bytes);
      throw e;
    }
    ByteArray byteArray = new ByteArray();
    byteArray.setArray(bytes);
    byteArray.setStart(0);
    byteArray.setSize(size);
    byteArray.setMetaData(metaData);
    LOG.info("Byte array size: " + size + ", real array size: " + bytes.length
      + ", meta data size: " + metaDataSize);
    return byteArray;
  }

  private void receiveBytes(DataInputStream din,
    DataOutputStream doutNextWorker, byte[] bytes, int size) throws IOException {
    // Receive bytes data and process
    int recvLen = 0;
    int len = 0;
    while ((len = din.read(bytes, recvLen, Constants.SENDRECV_BYTE_UNIT)) > 0) {
      if (doutNextWorker != null) {
        doutNextWorker.write(bytes, recvLen, len);
        doutNextWorker.flush();
      }
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
