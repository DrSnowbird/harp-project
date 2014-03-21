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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

public class LongDblVtxPartition extends StructPartition {

  private Long2DoubleOpenHashMap vertexMap;

  public LongDblVtxPartition() {  
  }
  
  public void initialize(int partitionID, int expVertexCount) {
    this.setPartitionID(partitionID);
    if (this.vertexMap != null) {
      this.vertexMap.clear();
    } else {
      this.vertexMap = new Long2DoubleOpenHashMap(expVertexCount);
      this.vertexMap.defaultReturnValue(0);
    }
  }
  
  public LongDblVtxPartition(int partitionID, int expectedVertexCount) {
    super(partitionID);
    this.vertexMap = new Long2DoubleOpenHashMap(expectedVertexCount);
    this.vertexMap.defaultReturnValue(0);
  }

  public void addVertexVal(long vertexID, double vertexVal) {
    this.vertexMap.addTo(vertexID, vertexVal);
  }

  public void putVertexVal(long vertexID, double vertexVal) {
    this.vertexMap.put(vertexID, vertexVal);
  }

  public double getVertexVal(long vertexID) {
    return this.vertexMap.get(vertexID);
  }

  public int size() {
    return this.vertexMap.size();
  }

  public boolean isEmpty() {
    return this.vertexMap.isEmpty();
  }
  
  public Long2DoubleOpenHashMap getVertexMap() {
    return vertexMap;
  }

  @Override
  public int getSizeInBytes() {
    return 4 + 4 + vertexMap.size() * 16;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(vertexMap.size());
    for (Long2DoubleMap.Entry entry : vertexMap.long2DoubleEntrySet()) {
      out.writeLong(entry.getLongKey());
      out.writeDouble(entry.getDoubleValue());
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    this.setPartitionID(in.readInt());
    int size = in.readInt();
    if (vertexMap != null) {
      vertexMap.clear();
    } else {
      vertexMap = new Long2DoubleOpenHashMap(size);
    }
    this.vertexMap.defaultReturnValue(0);
    for (int i = 0; i < size; i++) {
      vertexMap.put(in.readLong(), in.readDouble());
    }
  }
}
