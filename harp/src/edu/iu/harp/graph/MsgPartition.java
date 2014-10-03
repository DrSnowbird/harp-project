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

package edu.iu.harp.graph;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.List;

import org.apache.log4j.Logger;

import edu.iu.harp.arrpar.Partition;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.resource.DataDeserializer;
import edu.iu.harp.comm.resource.DataSerializer;
import edu.iu.harp.comm.resource.ResourcePool;

/**
 * Message partition can be sent or receive In send process meta data are packed
 * as a struct object byte array data are marked with workerID, tableID,
 * partitionID, arrayID Receiver needs to build a pool to manage the data
 * received.
 * 
 * More complicated operations on message partitions include graph-based message
 * regroup, allgather
 * 
 * 
 * @author zhangbj
 * 
 * @param <I>
 * @param <M>
 */
public class MsgPartition<I extends VertexID, M extends MsgVal> implements
  Partition {

  protected static final Logger LOG = Logger.getLogger(MsgPartition.class);

  private int partitionID;
  private List<ByteArray> arrays;
  private int expectedSize;
  // [0] array index [1] element index
  private int[] writePos;
  private int[] readPos;
  private Class<I> iClass;
  private I vertexID;
  private int vertexIDSize;
  private Class<M> mClass;
  private M msgVal;
  private int msgValSize;
  private ResourcePool resourcePool;

  public MsgPartition(int pID, ResourcePool pool, Class<I> iClass,
    Class<M> mClass, int expectedSize) {
    partitionID = pID;
    arrays = new ObjectArrayList<ByteArray>(5);
    writePos = new int[2];
    readPos = new int[2];
    this.resourcePool = pool;
    this.expectedSize = expectedSize;
    this.iClass = iClass;
    this.mClass = mClass;
  }
  
  /**
   * Write a message at the current writePos.
   * 
   * @param source
   * @param val
   * @param target
   */
  public void addMsg(I id, M val) {
    int idSize = id.getSizeInBytes();
    int msgSize = val.getSizeInBytes();
    // If no array
    if (this.arrays.isEmpty()) {
      if (expectedSize < (idSize + msgSize)) {
        expectedSize = idSize + msgSize;
      }
      byte[] bytes = this.resourcePool.getByteArrayPool()
        .getArray(expectedSize);
      ByteArray array = new ByteArray();
      array.setArray(bytes);
      array.setSize(0);
      arrays.add(array);
      writePos[0] = 0;
      writePos[1] = 0;
    }
    ByteArray array = this.arrays.get(writePos[0]);
    // Since write is an "append" operation (you can not write at the middle)
    // we assume the array ahead is always available (to the end of bytes[])
    int availSize = array.getArray().length - array.getStart();
    if ((writePos[1] + idSize + msgSize) > availSize) {
      // Set end of this array
      array.setSize(writePos[1]);
      writePos[0]++;
      // Add array to arrays
      if (writePos[0] == this.arrays.size()) {
        byte[] bytes = this.resourcePool.getByteArrayPool().getArray(
          expectedSize);
        array = new ByteArray();
        array.setArray(bytes);
        array.setSize(0); // the current usage of the array
        this.arrays.add(array);
      }
      writePos[1] = 0;
    }
    boolean exception = false;
    DataSerializer serializer = new DataSerializer(array.getArray(),
      array.getStart() + writePos[1]);
    try {
      id.write(serializer);
      val.write(serializer);
    } catch (Exception e) {
      LOG.error("Error when write", e);
      exception = true;
    }
    if (!exception) {
      writePos[1] = writePos[1] + idSize + msgSize;
      array.setSize(writePos[1]);
    }
  }

  /**
   * If no msg is held, read from the current pos. Else, jump current msg, read
   * next.
   */
  public boolean nextMsg() {
    // If we are holding a msg value, jump to next read position,
    // else, read from the current position
    if (vertexID != null && msgVal != null) {
      readPos[1] = readPos[1] + vertexIDSize + msgValSize;
      // If we arrive to the end of msg partition (marked by writePos)
      // We know writePos stops at the end of last write
      // If read is ahead of write, there is no data.
      if (readPos[0] == writePos[0] && readPos[1] >= writePos[1]) {
        releaseMsgObject();
        return false;
      }
      ByteArray array = arrays.get(readPos[0]);
      // If it is at the end of an array, go to next array.
      if (readPos[1] == array.getSize()) {
        readPos[0]++;
        // Go to next array,
        // Must not be null, because write is ahead of read
        array = this.arrays.get(readPos[0]);
        readPos[1] = 0;
      }
    } else {
      // May be no new things are written
      if ((readPos[0] > writePos[0])
        || (readPos[0] == writePos[0] && readPos[1] >= writePos[1])) {
        LOG.info("NO MSG");
        return false;
      }
    }
    // LOG.info("READING MSG");
    ByteArray array = this.arrays.get(readPos[0]);
    DataDeserializer deserializer = new DataDeserializer(array.getArray(),
      array.getStart() + readPos[1]);
    createMsgObject();
    try {
      vertexID.read(deserializer);
      vertexIDSize = vertexID.getSizeInBytes();
      msgVal.read(deserializer);
      msgValSize = msgVal.getSizeInBytes();
    } catch (Exception e) {
      LOG.error("Error when read", e);
      releaseMsgObject();
      return false;
    }
    return true;
  }

  private void createMsgObject() {
    // Same reason as edge object
    // new them directly instead of getting them from resource pool
    if (vertexID == null) {
      try {
        vertexID = this.iClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        vertexID = null;
      }
    }
    if (msgVal == null) {
      try {
        msgVal = this.mClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        msgVal = null;
      }
    }
  }

  private void releaseMsgObject() {
    if (vertexID != null && msgVal != null) {
      this.resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
        vertexID);
      this.resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
        msgVal);
    }
    vertexID = null;
    msgVal = null;
    vertexIDSize = 0;
    msgValSize = 0;
  }

  public I getCurVertexID() {
    return vertexID;
  }

  public M getCurMsgVal() {
    return msgVal;
  }

  public void addData(ByteArray array) {
    boolean isEmpty = this.arrays.isEmpty();
    if (!isEmpty) {
      this.arrays.add(0, array);
      writePos[0]++;
    } else {
      this.arrays.add(array);
      writePos[0] = 0;
      writePos[1] = this.arrays.get(0).getSize();
    }
    readPos[0] = 0;
    readPos[1] = 0;
    releaseMsgObject();
  }

  /**
   * Data are inserted at the beginning of the partition Readpos is restored to
   * the beginning too.
   * 
   * @param arrays
   */
  public void addAllData(List<ByteArray> arrays) {
    boolean isEmpty = this.arrays.isEmpty();
    if (!isEmpty) {
      this.arrays.addAll(0, arrays);
      writePos[0] = writePos[0] + arrays.size();
    } else {
      this.arrays.addAll(arrays);
      writePos[0] = this.arrays.size() - 1;
      writePos[1] = this.arrays.get(this.arrays.size() - 1).getSize();
    }
    readPos[0] = 0;
    readPos[1] = 0;
    releaseMsgObject();
  }

  public List<ByteArray> getData() {
    return this.arrays;
  }

  /**
   * Merge byte arrays Mark read/write pos to 0
   */
  public void clean() {
    readPos[0] = 0;
    readPos[1] = 0;
    writePos[0] = 0;
    writePos[0] = 0;
    if (vertexID != null && msgVal != null) {
      releaseMsgObject();
    }
    ObjectOpenHashSet<byte[]> byteArrays = new ObjectOpenHashSet<byte[]>(
      this.arrays.size());
    for (ByteArray array : this.arrays) {
      byteArrays.add(array.getArray());
    }
    this.arrays.clear();
    for (byte[] bytes : byteArrays) {
      ByteArray array = new ByteArray();
      array.setArray(bytes);
      array.setSize(0);
      this.arrays.add(array);
    }
  }

  public void release() {
    clean();
    for (ByteArray byteArray : this.arrays) {
      this.resourcePool.getByteArrayPool().releaseArrayInUse(
        byteArray.getArray());
      this.resourcePool.getIntArrayPool().releaseArrayInUse(
        byteArray.getMetaArray());
    }
  }

  @Override
  public int getPartitionID() {
    return this.partitionID;
  }

  @Override
  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }
}
