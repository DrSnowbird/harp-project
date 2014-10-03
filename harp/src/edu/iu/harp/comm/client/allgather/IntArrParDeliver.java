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
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.IntArrReqSender;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class IntArrParDeliver extends IntArrReqSender {

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(IntArrParDeliver.class);

  private final int workerID;
  private final int tableID;
  private final int partitionID;

  public IntArrParDeliver(Workers workers, ResourcePool pool, int tableID,
    ArrPartition<IntArray> partition) {
    // Make sure that next worker is not self.
    // Control this in the benchmark driver program
    super(workers.getNextInfo().getNode(), workers.getNextInfo().getPort(),
      partition.getArray(), pool);
    this.workerID = workers.getSelfID();
    this.tableID = tableID;
    this.partitionID = partition.getPartitionID();
    // LOG.info("workerID: " + workerID + ", partitionID: " + partitionID);
    this.setCommand(Constants.BYTE_ARRAY_REQUEST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    ByteArray byteArray = (ByteArray) super.processData(data);
    int[] metaArray = this.getResourcePool().getIntArrayPool().getArray(3);
    metaArray[0] = workerID;
    metaArray[1] = tableID;
    metaArray[2] = partitionID;
    byteArray.setMetaArray(metaArray);
    byteArray.setMetaArraySize(3);
    return byteArray;
  }
}
