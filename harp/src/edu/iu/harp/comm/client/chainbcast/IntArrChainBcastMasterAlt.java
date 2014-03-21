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

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class IntArrChainBcastMasterAlt extends ChainBcastMaster {

  public IntArrChainBcastMasterAlt(Commutable data, Workers workers,
    ResourcePool resourcePool) throws Exception {
    super(data, workers, resourcePool);
    this.setCommand(Constants.INT_ARRAY_CHAIN_BCAST_ALT);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    // This should be int array
    IntArray intArray = (IntArray) this.getData();
    return intArray;
  }

  @Override
  protected void sendProcessedData(Connection conn, Commutable data)
    throws Exception {
    // We observed this had bad performance
    // The implementation ignores the meta data in int array
    IntArray intArray = (IntArray) this.getData();
    int[] ints = intArray.getArray();
    int size = intArray.getSize();
    DataOutputStream dout = conn.getDataOutputStream();
    // Send meta data
    dout.writeByte(this.getCommand());
    dout.writeInt(size);
    dout.flush();
    // Send data
    int start = 0;
    int sendUnit = Constants.SENDRECV_INT_UNIT;
    while ((start + sendUnit) <= size) {
      for (int i = 0; i < sendUnit; i++) {
        dout.writeInt(ints[start + i]);
      }
      start = start + sendUnit;
      dout.flush();
    }
    // Send the rest
    if (start < size) {
      for (int i = start; i < size; i++) {
        dout.writeInt(ints[i]);
      }
      dout.flush();
    }
  }

  @Override
  protected void releaseProcessedData(Commutable data) {
  }
}
