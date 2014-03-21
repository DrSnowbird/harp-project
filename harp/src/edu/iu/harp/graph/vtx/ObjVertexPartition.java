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

import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import edu.iu.harp.arrpar.Partition;
import edu.iu.harp.graph.VertexID;

/**
 * a vertex partition stores vertex ID and vertex value, we
 * 
 * @author zhangbj
 * 
 * @param <I>
 * @param <V>
 */

public class ObjVertexPartition<I extends VertexID, V extends VertexVal>
  extends StructPartition implements Partition {

  private Object2ObjectOpenHashMap<I, V> vertexMap;
  private Class<I> iClass;
  private Class<V> vClass;

  public ObjVertexPartition(int partitionID, int expectedVertexCount,
    Class<I> iClass, Class<V> vClass) {
    super(partitionID);
    this.vertexMap = new Object2ObjectOpenHashMap<I, V>(expectedVertexCount);
    this.iClass = iClass;
    this.vClass = vClass;
  }

  public void setIClass(Class<I> iClass) {
    this.iClass = iClass;
  }

  public Class<I> getIClass() {
    return this.iClass;
  }

  public void setVClass(Class<V> vClass) {
    this.vClass = vClass;
  }

  public Class<V> getVClass() {
    return this.vClass;
  }

  public boolean addVertex(I i, V v) {
    if (!vertexMap.containsKey(i)) {
      vertexMap.put(i, v);
      return true;
    } else {
      return false;
    }
  }

  public V getVertex(I vertexID) {
    return this.vertexMap.get(vertexID);
  }

  public int size() {
    return this.vertexMap.size();
  }

  public boolean isEmpty() {
    return this.vertexMap.isEmpty();
  }
  
  public Object2ObjectOpenHashMap<I, V> getVertexMap() {
    return this.vertexMap;
  }

  @Override
  public int getSizeInBytes() {
    int size = 4 + 4; // partition Id + mapSize
    size = size + 4 + this.iClass.getName().length() * 2;
    size = size + 4 + this.vClass.getName().length() * 2;
    for (Object2ObjectMap.Entry<I, V> entry : vertexMap.object2ObjectEntrySet()) {
      size = size + entry.getKey().getSizeInBytes()
        + entry.getValue().getSizeInBytes();
    }
    return size;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(vertexMap.size());
    out.writeUTF(this.iClass.getName());
    out.writeUTF(this.vClass.getName());
    for (Object2ObjectMap.Entry<I, V> entry : vertexMap.object2ObjectEntrySet()) {
      entry.getKey().write(out);
      entry.getValue().write(out);
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    this.setPartitionID(in.readInt());
    int size = in.readInt();
    try {
      this.iClass = (Class<I>) Class.forName(in.readUTF());
      this.vClass = (Class<V>) Class.forName(in.readUTF());
      this.vertexMap = new Object2ObjectOpenHashMap<I, V>(size);
      for (int k = 0; k < size; k++) {
        I i = this.iClass.newInstance();
        i.read(in);
        V v = this.vClass.newInstance();
        v.read(in);
        this.vertexMap.put(i, v);
      }
    } catch (ClassNotFoundException | InstantiationException
      | IllegalAccessException e) {
      throw new IOException(e);
    }
  }
}
