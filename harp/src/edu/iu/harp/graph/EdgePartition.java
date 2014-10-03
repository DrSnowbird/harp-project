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

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import edu.iu.harp.arrpar.Partition;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.resource.DataDeserializer;
import edu.iu.harp.comm.resource.DataSerializer;
import edu.iu.harp.comm.resource.ResourcePool;

/**
 * Assume input is binary file for in-edge with the following format id
 * target_id_1 edge_value target_id_2 edge_value
 * 
 * Edge partition is binary based. edge can be read out from partition as
 * object or written to partition as binary.
 * 
 * Support add/remove, read/write
 * 
 * But it is difficult to check if there is duplication.... assume no
 * duplication
 * 
 * @author zhangbj
 * 
 */
public class EdgePartition<I extends VertexID, E extends EdgeVal> implements Partition {

  protected static final Logger LOG = Logger.getLogger(EdgePartition.class);
  
  private int partitionID;
  private List<ByteArray> arrays;
  private int expectedSize;
  // [0] array index [1] element index
  private int[] writePos;
  private int[] readPos;
  private Class<I> iClass;
  private I sourceID;
  private int sourceIDSize;
  private I targetID;
  private int targetIDSize;
  private Class<E> eClass;
  private E edgeVal;
  private int edgeValSize;
  private ResourcePool resourcePool;

  public EdgePartition(int pID, ResourcePool pool, Class<I> iClass,
    Class<E> eClass, int expectedSize) {
    partitionID = pID;
    arrays = new ObjectArrayList<ByteArray>(5);
    writePos = new int[2];
    writePos[0] = 0;
    writePos[1] = 0;
    readPos = new int[2];
    readPos[0] = 0;
    readPos[1] = 0;
    this.resourcePool = pool;
    this.expectedSize = expectedSize;
    this.iClass = iClass;
    this.eClass = eClass;
  }

  /**
   * Write an edge at current pos.
   * 
   * @param source
   * @param val
   * @param target
   */
  public void addEdge(I source, E val, I target) {
    if (arrays.isEmpty()) {
      byte[] bytes = this.resourcePool.getByteArrayPool()
        .getArray(expectedSize);
      ByteArray array = new ByteArray();
      array.setArray(bytes);
      // At the beginning, no contents, so no size
      array.setSize(0);
      arrays.add(array);
    }
    int sourceSize = source.getSizeInBytes();
    int edgeSize = val.getSizeInBytes();
    int targetSize = target.getSizeInBytes();
    ByteArray array = this.arrays.get(writePos[0]);
    int availSize = array.getArray().length - array.getStart();
    if ((writePos[1] + sourceSize + edgeSize + targetSize) > availSize) {
      // Set end of this array
      array.setSize(writePos[1]);
      writePos[0]++;
      if (writePos[0] == this.arrays.size()) {
        // Add array to arrays
        byte[] bytes = this.resourcePool.getByteArrayPool().getArray(
          expectedSize);
        array = new ByteArray();
        array.setArray(bytes);
        array.setSize(0); // Array size is consistent with used size
        this.arrays.add(array);
      }
      writePos[1] = 0;
    }
    boolean exception = false;
    DataSerializer serializer = new DataSerializer(array.getArray(),
      array.getStart() + writePos[1]);
    try {
      source.write(serializer);
      val.write(serializer);
      target.write(serializer);
    } catch (Exception e) {
      LOG.error("Error when write", e);
      exception = true;
    }
    if (!exception) {
      writePos[1] = writePos[1] + sourceSize + edgeSize + targetSize;
      array.setSize(writePos[1]);
    }
  }

  /**
   * Reading stops the current edge read position, when read next edge, jump to
   * next read position. 
   */
  public boolean nextEdge() {
    // If we are holding some edge values, jump to next read position,
    // else, read from the current position
    if (sourceID != null && edgeVal != null && targetID != null) {
      readPos[1] = readPos[1] + sourceIDSize + edgeValSize + targetIDSize;
      // If we arrive to the end of edge partition (marked by writePos)
      // We know write pos stops at the end of last write
      // If read is ahead of write, there is no data.
      if (readPos[0] == writePos[0] && readPos[1] >= writePos[1]) {
        releaseEdgeObject();
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
        return false;
      }
    }
    ByteArray array = this.arrays.get(readPos[0]);
    DataDeserializer deserializer = new DataDeserializer(array.getArray(),
      array.getStart() + readPos[1]);
    createEdgeObject();
    try {
      sourceID.read(deserializer);
      sourceIDSize = sourceID.getSizeInBytes();
      edgeVal.read(deserializer);
      edgeValSize = edgeVal.getSizeInBytes();
      targetID.read(deserializer);
      targetIDSize = targetID.getSizeInBytes();
    } catch (IOException e) {
      LOG.error("Error when read", e);
      releaseEdgeObject();
      return false;
    }
    return true;
  }
  
  private void createEdgeObject() {
    // Writable object pool use hash set to maintain the object cache
    // however, for LongVertexID, hashcode may change. If then ,they can
    // not be found in the set and got released. So here we new objects
    // directly.
    if (sourceID == null) {
      try {
        sourceID = this.iClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        sourceID = null;
      }
    }
    if (edgeVal == null) {
      try {
        edgeVal = this.eClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        edgeVal = null;
      }
    }
    if (targetID == null) {
      try {
        targetID = this.iClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        targetID = null;
      }
    }
  }
  
  private void releaseEdgeObject() {
    if (sourceID != null && edgeVal != null && targetID != null) {
      this.resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
        sourceID);
      this.resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
        edgeVal);
      this.resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
        targetID);
    }
    sourceID = null;
    edgeVal = null;
    targetID = null;
    sourceIDSize = 0;
    edgeValSize = 0;
    targetIDSize = 0;
  }

  public I getCurSourceID() {
    return sourceID;
  }

  public E getCurEdgeVal() {
    return edgeVal;
  }

  public I getCurTargetID() {
    return targetID;
  }
  
  public List<ByteArray> getData() {
    return this.arrays;
  }

  public void saveCurEdge() {
    if (sourceID == null && edgeVal == null && targetID == null) {
      return;
    }
    if (sourceIDSize != sourceID.getSizeInBytes()
      || edgeValSize != edgeVal.getSizeInBytes()
      || targetIDSize != targetID.getSizeInBytes()) {
      return;
    }
    ByteArray array = arrays.get(readPos[0]);
    DataSerializer serializer = new DataSerializer(array.getArray(),
      array.getStart() + readPos[1]);
    try {
      sourceID.write(serializer);
      edgeVal.write(serializer);
      targetID.write(serializer);
    } catch (Exception e) {
      LOG.error("Error when save", e);
    }
  }

  public void removeCurEdge() {
    // If no current edge
    if (sourceID == null && edgeVal == null && targetID == null) {
      return;
    }
    ByteArray array = this.arrays.get(readPos[0]);
    int newStart = array.getStart() + readPos[1] + sourceIDSize + edgeValSize
      + targetIDSize;
    if (newStart == array.getStart() + array.getSize()) {
      // If reach the end of the array
      array.setSize(readPos[1]);
      // If write and read are on the same array
      if (writePos[0] == readPos[0]) {
        writePos[1] = readPos[1];
      }
      return;
    } else if (readPos[1] == 0) {
      // If it is at the beginning of the array
      array.setStart(newStart);
      // readPos[0] is the same
      // readPos[1] should still be 0
      // writePos should be ahead of readPos
      // the distance between newStart and original start
      // is sourceIDSize + edgeValSize + targetIDSize
      if (writePos[0] == readPos[0]) {
        writePos[1] = writePos[1] - sourceIDSize - edgeValSize - targetIDSize;
        array.setSize(writePos[1]);
      }
    } else {
      // In normal cases
      ByteArray newArray = new ByteArray();
      newArray.setArray(array.getArray());
      newArray.setStart(newStart);
      newArray.setSize(array.getStart() + array.getSize() - newStart);
      this.arrays.add(readPos[0] + 1, newArray);
      array.setSize(readPos[1]);
      if (writePos[0] == readPos[0]) {
        writePos[0]++;
        writePos[1] = array.getStart() + writePos[1] - newStart;
        arrays.get(writePos[0]).setSize(writePos[1]);
      }
      readPos[0]++;
      readPos[1] = 0;
    }
    releaseEdgeObject();
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
    releaseEdgeObject();
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
    releaseEdgeObject();
  }
  
  /**
   * Merge byte arrays
   * Mark read/write pos to 0
   */
  public void clean() {
    readPos[0] = 0;
    readPos[1] = 0;
    writePos[0] = 0;
    writePos[0] = 0;
    if (sourceID != null && edgeVal != null && targetID != null) {
      releaseEdgeObject();
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
      // No need to release meta data
      // In sending, meta data are added, then meta array are removed
      // In receiving, meta array are got, then are removed.
      this.resourcePool.getByteArrayPool().releaseArrayInUse(
        byteArray.getArray());
    }
  }
  
  public void defaultReadPos() {
    if (sourceID != null && edgeVal != null && targetID != null) {
      releaseEdgeObject();
    }
    readPos[0] = 0;
    readPos[1] = 0;
  }

  @Override
  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }

  @Override
  public int getPartitionID() {
    return this.partitionID;
  }
}
