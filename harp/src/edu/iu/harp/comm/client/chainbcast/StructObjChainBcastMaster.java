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
import edu.iu.harp.comm.client.StructObjReqSender;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.StructObject;
import edu.iu.harp.comm.resource.ResourcePool;

public class StructObjChainBcastMaster extends ByteArrChainBcastMaster {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(StructObjChainBcastMaster.class);

  public StructObjChainBcastMaster(Commutable data, Workers workers,
    ResourcePool pool) throws Exception {
    super(data, workers, pool);
    this.setCommand(Constants.STRUCT_OBJ_CHAIN_BCAST);
  }

  @Override
  protected Commutable processData(Commutable data) throws Exception {
    // Should be struct object
    StructObject obj = (StructObject) data;
    // Convert a struct object to a byte array with meta data
    String className = obj.getClass().getName();
    // Obj size + class name size (including characters and the total length)
    int size = obj.getSizeInBytes() + className.length() * 2 + 4;
    // LOG
    //  .info("Struct Obj class name: " + className + ", size in bytes: " + size);
    // Serialize to bytes
    byte[] bytes = this.getResourcePool().getByteArrayPool().getArray(size);
    try {
      StructObjReqSender.serializeStructObjToBytes(className, obj, bytes);
    } catch (Exception e) {
      this.getResourcePool().getByteArrayPool().releaseArrayInUse(bytes);
      throw e;
    }
    // Create byte array
    ByteArray array = new ByteArray();
    array.setArray(bytes);
    array.setMetaArray(null);
    array.setSize(size);
    array.setStart(0);
    return array;
  }
}
