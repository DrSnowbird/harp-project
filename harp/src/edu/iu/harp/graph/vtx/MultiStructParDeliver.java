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

package edu.iu.harp.graph.vtx;

import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.client.StructObjReqSender;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.request.MultiStructPartition;
import edu.iu.harp.comm.resource.ResourcePool;

public class MultiStructParDeliver<P extends StructPartition> extends
  StructObjReqSender {

  private int workerID;
  private int tableID;
  private int partitionCount;

  public MultiStructParDeliver(int workerID, String nextHost, int nextPort,
    int tableID, MultiStructPartition multiPartition, ResourcePool pool) {
    super(nextHost, nextPort, multiPartition, pool);
    this.workerID = workerID;
    this.tableID = tableID;
    this.partitionCount = multiPartition.getNumPartitions();
    this.setCommand(Constants.BYTE_ARRAY_REQUEST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    ByteArray byteArray = (ByteArray) super.processData(data);
    int[] metaArray = this.getResourcePool().getIntArrayPool().getArray(3);
    // The order of elements of meta array is the same across of kinds of
    // send/recv
    metaArray[0] = workerID;
    metaArray[1] = tableID;
    metaArray[2] = partitionCount;
    byteArray.setMetaArray(metaArray);
    byteArray.setMetaArraySize(3);
    return byteArray;
  }
}
