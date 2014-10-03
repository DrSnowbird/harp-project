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

package edu.iu.harp.keyval;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.client.StructObjReqSender;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;

public class KeyValParSender<K extends Key, V extends Value, C extends ValCombiner<V>>
  extends StructObjReqSender {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(KeyValParSender.class);
  private int partitionID;

  public KeyValParSender(String host, int port,
    KeyValPartition<K, V, C> partition, ResourcePool pool) {
    super(host, port, partition, pool);
    this.partitionID = partition.getPartitionID();
    LOG.info("Partition ID: " + partitionID + " Size: "
      + partition.getSizeInBytes());
    this.setCommand(Constants.BYTE_ARRAY_REQUEST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    ByteArray byteArray = (ByteArray) super.processData(data);
    int[] metaArray = this.getResourcePool().getIntArrayPool().getArray(1);
    metaArray[0] = this.partitionID;
    byteArray.setMetaArray(metaArray);
    byteArray.setMetaArraySize(1);
    return byteArray;
  }
}
