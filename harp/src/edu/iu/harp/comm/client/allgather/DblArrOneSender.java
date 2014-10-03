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

package edu.iu.harp.comm.client.allgather;

import org.apache.log4j.Logger;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.client.DoubleArrReqSender;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

/**
 * Currently we build the logic based on the simple logic. No fault tolerance is
 * considered.
 * 
 */
public class DblArrOneSender extends DoubleArrReqSender {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(DblArrOneSender.class);

  private int selfID;
  private int tableID;

  public DblArrOneSender(String destHost, int destPort, int selfID,
    int tableID, ArrPartition<DoubleArray> partition, ResourcePool pool) {
    super(destHost, destPort, partition.getArray(), pool);
    this.selfID = selfID;
    this.tableID = tableID;
    this.setCommand(Constants.BYTE_ARRAY_REQUEST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    ByteArray array = (ByteArray) super.processData(data);
    int[] metaArray = this.getResourcePool().getIntArrayPool().getArray(2);
    metaArray[0] = selfID;
    metaArray[1] = tableID;
    array.setMetaArray(metaArray);
    array.setMetaArraySize(2);
    return array;
  }
}
