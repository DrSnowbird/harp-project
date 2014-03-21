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

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class IntArrChainBcastHandlerAlt extends ChainBcastHandler {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(IntArrChainBcastHandlerAlt.class);

  public IntArrChainBcastHandlerAlt(WorkerData workerData, ResourcePool pool,
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
    int size = din.readInt();
    if (doutNextWorker != null) {
      doutNextWorker.writeByte(this.getCommand());
      doutNextWorker.writeInt(size);
      doutNextWorker.flush();
    }
    int[] ints = this.getResourcePool().getIntArrayPool().getArray(size);
    LOG.info("Int array size: " + size + ", real array size: " + ints.length);
    // Send
    int recvStart = 0;
    int sendStart = 0;
    int recvUnit = Constants.SENDRECV_INT_UNIT;
    try {
      while ((recvStart + recvUnit) <= size) {
        for (int i = 0; i < recvUnit; i++) {
          ints[recvStart + i] = din.readInt();
        }
        sendStart = recvStart;
        recvStart = recvStart + recvUnit;
        if (doutNextWorker != null) {
          for (int i = 0; i < recvUnit; i++) {
            doutNextWorker.writeInt(ints[sendStart + i]);
          }
          doutNextWorker.flush();
        }
      }
      // Send the rest
      if (recvStart < size) {
        for (int i = recvStart; i < size; i++) {
          ints[i] = din.readInt();
        }
        if (doutNextWorker != null) {
          for (int i = recvStart; i < size; i++) {
            doutNextWorker.writeInt(ints[i]);
          }
          doutNextWorker.flush();
        }
      }
    } catch (Exception e) {
      this.getResourcePool().getIntArrayPool().releaseArrayInUse(ints);
      throw e;
    }
    // Close connection
    conn.close();
    if (nextConn != null) {
      nextConn.close();
    }
    IntArray array = new IntArray();
    array.setArray(ints);
    array.setSize(size);
    return array;
  }
}
