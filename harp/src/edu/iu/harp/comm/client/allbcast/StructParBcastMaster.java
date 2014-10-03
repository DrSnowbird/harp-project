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

package edu.iu.harp.comm.client.allbcast;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.chainbcast.StructObjChainBcastMaster;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.graph.vtx.StructPartition;

public class StructParBcastMaster<P extends StructPartition> extends
  StructObjChainBcastMaster {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(StructParBcastMaster.class);

  private int partitionID;

  public StructParBcastMaster(Workers workers, ResourcePool pool, P partition)
    throws Exception {
    super(partition, workers, pool);
    partitionID = partition.getPartitionID();
    LOG.info("partitionID: " + partitionID);
    this.setCommand(Constants.BYTE_ARRAY_CHAIN_BCAST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    ByteArray array = (ByteArray) super.processData(data);
    // int[] metaData = new int[1];
    int[] metaArray = this.getResourcePool().getIntArrayPool().getArray(1);
    metaArray[0] = this.partitionID;
    array.setMetaArray(metaArray);
    array.setMetaArraySize(1);
    return array;
  }
}
