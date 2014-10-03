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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

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
public class IntFltArrVtxPartition extends IntAbstractObjVtxPartition<float[]> {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(IntFltArrVtxPartition.class);

  private int arrLen;

  public IntFltArrVtxPartition() {
  }

  public void initialize(int partitionID, int expVtxCount, int arrLen) {
    super.initialize(partitionID, expVtxCount, float[].class);
    this.arrLen = arrLen;
  }

  public IntFltArrVtxPartition(int partitionID, int expVtxCount, int arrLen) {
    super(partitionID, expVtxCount, float[].class);
    this.arrLen = arrLen;
  }

  private boolean checkArrLen(float[] vertexVal) {
    if (vertexVal.length != this.arrLen) {
      LOG.info("vertexVal doesn't match with partition definiation." + " "
        + vertexVal.length + " " + this.arrLen);
      return false;
    }
    return true;
  }

  @Override
  public boolean initVertexVal(int vertexID, float[] vertexVal) {
    if (!checkArrLen(vertexVal)) {
      return false;
    }
    float[] fltArr = this.getVertexMap().get(vertexID);
    if (fltArr == null) {
      putNewVtxVal(vertexID, vertexVal);
    }
    return true;
  }

  @Override
  public boolean addVertexVal(int vertexID, float[] vertexVal) {
    if (!checkArrLen(vertexVal)) {
      return false;
    }
    float[] dblArr = this.getVertexMap().get(vertexID);
    if (dblArr == null) {
      putNewVtxVal(vertexID, vertexVal);
    } else {
      for (int i = 0; i < this.arrLen; i++) {
        dblArr[i] = dblArr[i] + vertexVal[i];
      }
    }
    return true;
  }

  @Override
  public boolean putVertexVal(int vertexID, float[] vertexVal) {
    if (!checkArrLen(vertexVal)) {
      return false;
    }
    float[] fltArr = this.getVertexMap().get(vertexID);
    if (fltArr == null) {
      putNewVtxVal(vertexID, vertexVal);
    } else {
      System.arraycopy(vertexVal, 0, fltArr, 0, fltArr.length);
    }
    return true;
  }

  private void putNewVtxVal(int vertexID, float[] vertexVal) {
    float[] fltArr = new float[vertexVal.length];
    System.arraycopy(vertexVal, 0, fltArr, 0, vertexVal.length);
    this.getVertexMap().put(vertexID, fltArr);
  }

  public int getArrayLength() {
    return this.arrLen;
  }

  @Override
  public int getSizeInBytes() {
    int size = 4 + 4 + 4; // partition Id + arrLen + mapSize
    // Key ID + each element size
    size = size + (4 + this.arrLen * 4) * this.getVertexMap().size();
    return size;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(this.arrLen);
    out.writeInt(this.getVertexMap().size());
    float[] floats = null;
    for (Int2ObjectMap.Entry<float[]> entry : this.getVertexMap()
      .int2ObjectEntrySet()) {
      out.writeInt(entry.getIntKey());
      floats = entry.getValue();
      for (int i = 0; i < this.arrLen; i++) {
        out.writeFloat(floats[i]);
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
      this.createVertexMap(size, float[].class);
    }
    int key;
    float[] floats = null;
    for (int i = 0; i < size; i++) {
      key = in.readInt();
      floats = new float[this.arrLen];
      for (int j = 0; j < this.arrLen; j++) {
        floats[j] = in.readFloat();
      }
      this.getVertexMap().put(key, floats);
    }
  }
}
