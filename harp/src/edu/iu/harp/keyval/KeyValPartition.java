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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.iu.harp.arrpar.Partition;
import edu.iu.harp.comm.data.StructObject;

public class KeyValPartition<K extends Key, V extends Value, C extends ValCombiner<V>>
  extends StructObject implements Partition {

  protected static final Logger LOG = Logger.getLogger(KeyValPartition.class);

  private int partitionID;
  private Object2ObjectOpenHashMap<K, V> keyValMap;
  private C combiner;
  private Class<K> kClass;
  private Class<V> vClass;

  public KeyValPartition() {
    partitionID = 0;
    keyValMap = new Object2ObjectOpenHashMap<K, V>();
    combiner = null;
    kClass = null;
    vClass = null;
  }

  public KeyValPartition(int partitionID, int keyValSize, Class<K> kClass,
    Class<V> vClass, Class<C> cClass) {
    this.partitionID = partitionID;
    this.keyValMap = new Object2ObjectOpenHashMap<K, V>(keyValSize);
    setKClass(kClass);
    setVClass(vClass);
    setCombiner(cClass);
  }

  private void setKClass(Class<K> kClass) {
    this.kClass = kClass;
  }

  public Class<K> getKClass() {
    return this.kClass;
  }

  private void setVClass(Class<V> vClass) {
    this.vClass = vClass;
  }

  public Class<V> getVClass() {
    return this.vClass;
  }

  private void setCombiner(Class<C> cClass) {
    try {
      combiner = cClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      LOG.error("Fail to initialize combiner.", e);
      e.printStackTrace();
    }
  }

  public C getCombiner() {
    return this.combiner;
  }

  public boolean isEmpty() {
    return this.keyValMap.size() == 0;
  }

  public void addKeyVal(K k, V v) {
    V val = keyValMap.get(k);
    if (val == null) {
      keyValMap.put(k, v);
    } else {
      // Try to combine v to value
      combiner.combine(val, v);
    }
  }

  public void addKeyVals(Map<K, V> keyValMap) {
    for (Entry<K, V> entry : keyValMap.entrySet()) {
      addKeyVal(entry.getKey(), entry.getValue());
    }
  }

  public Map<K, V> getKeyValMap() {
    return keyValMap;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(keyValMap.size());
    if (keyValMap.size() > 0) {
      out.writeUTF(this.kClass.getName());
      out.writeUTF(this.vClass.getName());
      for (Entry<K, V> entry : keyValMap.entrySet()) {
        entry.getKey().write(out);
        entry.getValue().write(out);
      }
    }
    out.writeUTF(combiner.getClass().getName());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void read(DataInput in) throws IOException {
    int size = in.readInt();
    if (size > 0) {
      try {
        this.kClass = (Class<K>) Class.forName(in.readUTF());
        this.vClass = (Class<V>) Class.forName(in.readUTF());
        this.keyValMap = new Object2ObjectOpenHashMap<K, V>(size);
        for (int i = 0; i < size; i++) {
          K k = this.kClass.newInstance();
          V v = this.vClass.newInstance();
          k.read(in);
          v.read(in);
          this.keyValMap.put(k, v);
        }
      } catch (ClassNotFoundException | InstantiationException
        | IllegalAccessException e) {
        LOG.error("Fail to create keyval objects.", e);
      }
    }
    String combinerClassName = in.readUTF();
    try {
      combiner = (C) Class.forName(combinerClassName).newInstance();
    } catch (InstantiationException | IllegalAccessException
      | ClassNotFoundException e) {
      LOG.error("Fail to create combiner", e);
    }
  }

  @Override
  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }

  @Override
  public int getPartitionID() {
    return this.partitionID;
  }

  @Override
  public int getSizeInBytes() {
    // map size, key and val in map
    // combiner class name
    int size = 4;
    if (this.keyValMap.size() > 0) {
      size = size + this.kClass.getName().length() * 2 + 4;
      size = size + this.vClass.getName().length() * 2 + 4;
      for (Entry<K, V> entry : this.keyValMap.entrySet()) {
        size = size + entry.getKey().getSizeInBytes()
          + entry.getValue().getSizeInBytes();
      }
    }
    size = size + combiner.getClass().getName().length() * 2 + 4;
    return size;
  }
}
