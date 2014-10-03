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

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.IntArrReqSender;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class IntArrChainBcastMaster extends ByteArrChainBcastMaster {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(IntArrChainBcastMaster.class);

  public IntArrChainBcastMaster(Commutable data, Workers workers,
    ResourcePool pool) throws Exception {
    super(data, workers, pool);
    this.setCommand(Constants.INT_ARRAY_CHAIN_BCAST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    // Convert an int array to a byte array with meta data
    IntArray intArray = (IntArray) this.getData();
    int[] ints = intArray.getArray();
    int intsSize = intArray.getSize();
    int size = intsSize * 4 + 4;
    long start1 = System.currentTimeMillis();
    byte[] bytes = this.getResourcePool().getByteArrayPool().getArray(size);
    long start2 = System.currentTimeMillis();
    try {
      IntArrReqSender.serializeIntsToBytes(ints, intsSize, bytes);
    } catch (Exception e) {
      this.getResourcePool().getByteArrayPool().releaseArrayInUse(bytes);
      throw e;
    }
    long end = System.currentTimeMillis();
    LOG.info("Int array size " + intsSize + ". Create byte array time: "
      + (start2 - start1) + ". Serialization time:" + (end - start2));
    ByteArray byteArray = new ByteArray();
    byteArray.setArray(bytes);
    byteArray.setMetaArray(null);
    byteArray.setSize(size);
    byteArray.setStart(0);
    return byteArray;
  }
}
