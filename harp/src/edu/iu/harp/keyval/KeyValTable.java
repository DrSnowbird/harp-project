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

import java.util.Map.Entry;

import edu.iu.harp.arrpar.Table;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class KeyValTable<K extends Key, V extends Value, C extends ValCombiner<V>>
  implements Table {

  private int tableID;
  private final int numPartitions = 5;
  private Int2ObjectOpenHashMap<KeyValPartition<K, V, C>> partitions;
  private Class<K> kClass;
  private Class<V> vClass;
  private Class<C> cClass;

  public KeyValTable(int tableID, Class<K> kClass, Class<V> vClass,
    Class<C> cClass) {
    partitions = new Int2ObjectOpenHashMap<KeyValPartition<K, V, C>>();
    partitions.defaultReturnValue(null);
    this.tableID = tableID;
    this.kClass = kClass;
    this.vClass = vClass;
    this.cClass = cClass;
  }

  public int getTableID() {
    return this.tableID;
  }

  public int[] getPartitionIDs() {
    return partitions.keySet().toIntArray();
  }

  public Class<K> getKClass() {
    return this.kClass;
  }

  public Class<V> getVClass() {
    return this.vClass;
  }

  public Class<C> getCClass() {
    return this.cClass;
  }

  public KeyValPartition<K, V, C>[] getKeyValPartitions() {
    @SuppressWarnings("unchecked")
    KeyValPartition<K, V, C>[] parArray = new KeyValPartition[partitions.size()];
    return partitions.values().toArray(parArray);
  }

  public void addKeyVal(K k, V v) {
    int hashcode = Math.abs(k.hashCode());
    int partitionID = hashcode % numPartitions;
    KeyValPartition<K, V, C> partition = partitions.get(partitionID);
    if (partition == null) {
      partition = new KeyValPartition<K, V, C>(partitionID, 5, kClass, vClass,
        cClass);
      partitions.put(partitionID, partition);
    }
    partition.addKeyVal(k, v);
  }

  /**
   * Check if combining happens.
   * 
   * @param partition
   * @return Check if combining happens.
   */
  public boolean addKeyValPartition(KeyValPartition<K, V, C> partition) {
    int partitionID = partition.getPartitionID();
    KeyValPartition<K, V, C> currentPartition = this.partitions
      .get(partitionID);
    if (currentPartition != null) {
      currentPartition.addKeyVals(partition.getKeyValMap());
      return true;
    } else if (partitionID >= 0 && partitionID < numPartitions) {
      this.partitions.put(partitionID, partition);
      return false;
    } else {
      for (Entry<K, V> entry : partition.getKeyValMap().entrySet()) {
        addKeyVal(entry.getKey(), entry.getValue());
      }
      return true;
    }
  }
  
  public KeyValPartition<K, V, C> removeKeyValPartition(int partitionID) {
    return this.partitions.remove(partitionID);
  }
}
