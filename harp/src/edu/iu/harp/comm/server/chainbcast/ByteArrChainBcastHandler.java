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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.DataDeserializer;
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
    InputStream in = conn.getInputDtream();
    OutputStream outNextWorker = null;
    if (nextConn != null) {
      outNextWorker = nextConn.getOutputStream();
    }
    // Receive data
    ByteArray byteArray = receiveByteArray(in, outNextWorker);
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
      releaseInts(byteArray.getMetaArray());
      throw e;
    }
    return data;
  }

  /**
   * Receive 1. command 2. byte array size 3. meta data size 4. meta data
   * content
   * 
   * @param in
   * @param outNextWorker
   * @return
   * @throws Exception
   */
  private ByteArray receiveByteArray(InputStream in, OutputStream outNextWorker)
    throws Exception {
    // int size = din.readInt();
    // int metaDataSize = din.readInt();
    // Read byte array size and meta array size
    byte[] sizeBytes = this.getResourcePool().getByteArrayPool().getArray(8);
    in.read(sizeBytes);
    // Forward
    if (outNextWorker != null) {
      outNextWorker.write(this.getCommand());
      outNextWorker.write(sizeBytes);
    }
    DataDeserializer deserializer = new DataDeserializer(sizeBytes);
    int size = deserializer.readInt();
    int metaArraySize = deserializer.readInt();
    this.getResourcePool().getByteArrayPool().releaseArrayInUse(sizeBytes);
    // Read meta array
    int[] metaArray = null;
    if (metaArraySize > 0) {
      metaArray = this.getResourcePool().getIntArrayPool()
        .getArray(metaArraySize);
      byte[] metaArrayBytes = this.getResourcePool().getByteArrayPool()
        .getArray(4 * metaArraySize);
      in.read(metaArrayBytes);
      // Forward
      if (outNextWorker != null) {
        outNextWorker.write(metaArrayBytes);
      }
      deserializer.setData(metaArrayBytes);
      for (int i = 0; i < metaArraySize; i++) {
        metaArray[i] = deserializer.readInt();
      }
      this.getResourcePool().getByteArrayPool()
        .releaseArrayInUse(metaArrayBytes);
    }
    if (outNextWorker != null) {
      outNextWorker.flush();
    }
    // Prepare internal bytes structure
    byte[] bytes = this.getResourcePool().getByteArrayPool()
      .getArray(size + Constants.SENDRECV_BYTE_UNIT);
    try {
      receiveBytes(in, outNextWorker, bytes, size);
    } catch (Exception e) {
      releaseBytes(bytes);
      releaseInts(metaArray);
      throw e;
    }
    ByteArray byteArray = new ByteArray();
    byteArray.setArray(bytes);
    byteArray.setStart(0);
    byteArray.setSize(size);
    byteArray.setMetaArray(metaArray);
    byteArray.setMetaArraySize(metaArraySize);
    return byteArray;
  }

  private void receiveBytes(InputStream in, OutputStream outNextWorker,
    byte[] bytes, int size) throws IOException {
    // Receive bytes data and process
    int recvLen = 0;
    int len = 0;
    while ((len = in.read(bytes, recvLen, Constants.SENDRECV_BYTE_UNIT)) > 0) {
      if (outNextWorker != null) {
        outNextWorker.write(bytes, recvLen, len);
        outNextWorker.flush();
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

  private void releaseInts(int[] ints) {
    this.getResourcePool().getIntArrayPool().releaseArrayInUse(ints);
  }

  protected Commutable processByteArray(ByteArray array) throws Exception {
    return array;
  }
}
