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

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Each value is a double array... We assume it is not big, each of them just
 * have few elements.
 * 
 * @author zhangbj
 * 
 */
public class LongDblArrVtxPartition extends LongAbstractObjVtxPartition<double[]> {
  
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(LongDblArrVtxPartition.class);
  
  private int arrLen;

  public LongDblArrVtxPartition() {
  }
  
  public void initialize(int partitionID, int expVtxCount, int arrLen) {
    super.initialize(partitionID, expVtxCount, double[].class);
    this.arrLen = arrLen;
  }

  public LongDblArrVtxPartition(int partitionID, int expVtxCount, int arrLen) {
    super(partitionID, expVtxCount, double[].class);
    this.arrLen = arrLen;
  }

  private boolean checkArrLen(double[] vertexVal) {
    if (vertexVal.length != this.arrLen) {
      LOG.info("vertexVal doesn't match with partition definiation." + " "
        + vertexVal.length + " " + this.arrLen);
      return false;
    }
    return true;
  }

  @Override
  public boolean initVertexVal(long vertexID, double[] vertexVal) {
    if (!checkArrLen(vertexVal)) {
      return false;
    }
    double[] dblArr = this.getVertexMap().get(vertexID);
    if (dblArr == null) {
      putNewVtxVal(vertexID, vertexVal);
    }
    return true;
  }

  @Override
  public boolean addVertexVal(long vertexID, double[] vertexVal) {
    if (!checkArrLen(vertexVal)) {
      return false;
    }
    double[] dblArr = this.getVertexMap().get(vertexID);
    if (dblArr == null) {
      putNewVtxVal(vertexID, vertexVal);
    } else {
      for (int i = 0; i <  this.arrLen; i++) {
        dblArr[i] = dblArr[i] + vertexVal[i];
      }
    }
    return true;
  }

  @Override
  public boolean putVertexVal(long vertexID, double[] vertexVal) {
    if (!checkArrLen(vertexVal)) {
      return false;
    }
    double[] dblArr = this.getVertexMap().get(vertexID);
    if (dblArr == null) {
      putNewVtxVal(vertexID, vertexVal);
    } else {
      System.arraycopy(vertexVal, 0, dblArr, 0, dblArr.length);
    }
    return true;
  }

  private void putNewVtxVal(long vertexID, double[] vertexVal) {
    double[] dblArr = new double[vertexVal.length];
    System.arraycopy(vertexVal, 0, dblArr, 0, vertexVal.length);
    this.getVertexMap().put(vertexID, dblArr);
  }
  
  public int getArrayLength() {
    return this.arrLen;
  }

  @Override
  public int getSizeInBytes() {
    int size = 4 + 4 + 4; // partition Id + arrLen + mapSize
    // Key ID + each element size
    size = size + (8 + this.arrLen * 8) * this.getVertexMap().size();
    return size;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(this.arrLen);
    out.writeInt(this.getVertexMap().size());
    double[] doubles = null;
    for (Long2ObjectMap.Entry<double[]> entry : this.getVertexMap()
      .long2ObjectEntrySet()) {
      out.writeLong(entry.getLongKey());
      doubles = entry.getValue();
      for (int i = 0; i < this.arrLen; i++) {
        out.writeDouble(doubles[i]);
      }
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    this.setPartitionID(in.readInt());
    this.arrLen = in.readInt();
    int size = in.readInt();
    if (this.getVertexMap() != null) {
      int mapSize = this.getVertexMap().clean();
      // LOG.info("Real Map Size: " + size + " Map Container Size: " + mapSize);
    } else {
      this.createVertexMap(size, double[].class);
    }
    long key;
    double[] doubles = null;
    for (int i = 0; i < size; i++) {
      key = in.readLong();
      doubles = new double[this.arrLen];
      for (int j = 0; j < this.arrLen; j++) {
        doubles[j] = in.readDouble();
      }
      this.getVertexMap().put(key, doubles);
    }
  }
}
