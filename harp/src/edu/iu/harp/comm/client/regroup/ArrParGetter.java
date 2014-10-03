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

package edu.iu.harp.comm.client.regroup;

import org.apache.log4j.Logger;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.comm.CommUtil;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.DoubleArrReqHandler;
import edu.iu.harp.comm.server.IntArrReqHandler;

public class ArrParGetter<A extends Array<?>> {
  
  /** Class logger */
  protected static final Logger LOG = Logger.getLogger(ArrParGetter.class);
  
  private final WorkerData workerData;
  private final ResourcePool pool;
  private final Class<A> aClass;

  public ArrParGetter(WorkerData workerData, ResourcePool pool, Class<A> aClass) {
    this.workerData = workerData;
    this.pool = pool;
    this.aClass = aClass;
  }

  public ArrPartition<A> waitAndGet(long timeOut) throws Exception {
    // Wait for data arrival
    ByteArray byteArray = CommUtil.waitAndGet(this.workerData, ByteArray.class,
      timeOut, 500);
    if (byteArray == null) {
      LOG.error("ByteArray are not got.");
      return null;
    }
    ArrPartition<A> partition = null;
    A array = desiealizeToArray(byteArray, this.pool, this.aClass);
    // Meta data is the partition id
    int partitionID = byteArray.getMetaArray()[0];
    partition = new ArrPartition<A>(array, partitionID);
    this.pool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
    this.pool.getIntArrayPool().releaseArrayInUse(byteArray.getMetaArray());
    return partition;
  }

  @SuppressWarnings("unchecked")
  public static <A extends Array<?>> A desiealizeToArray(ByteArray byteArray,
    ResourcePool resourcePool, Class<A> aClass) throws Exception {
    A array = null;
    if (aClass.equals(IntArray.class)) {
      IntArray intArray = IntArrReqHandler.deserializeBytesToIntArray(
        byteArray.getArray(), resourcePool);
      if (intArray == null) {
        return null;
      } else {
        array = (A) intArray;
      }
    } else if (aClass.equals(DoubleArray.class)) {
      DoubleArray doubleArray = DoubleArrReqHandler
        .deserializeBytesToDoubleArray(byteArray.getArray(), resourcePool);
      if (doubleArray == null) {
        return null;
      } else {
        array = (A) doubleArray;
      }
    }
    return array;
  }
}
