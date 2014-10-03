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

import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;

public abstract class StructTable<P extends StructPartition> {
  protected int tableID;

  protected Int2ObjectAVLTreeMap<P> partitions;

  public StructTable(int tableID) {
    this.tableID = tableID;
    this.partitions = new Int2ObjectAVLTreeMap<P>();
  }

  public int getTableID() {
    return tableID;
  }

  public int[] getPartitionIDs() {
    return partitions.keySet().toIntArray();
  }
  
  public int getNumPartitions() {
    return this.partitions.size();
  }

  /**
   * Add a struct partition into a struct table
   * 
   * @param partition
   * @return 0: cannot add, 1: add, no merge 3, add and merge
   */
  public int addPartition(P partition) {
    if (!checkAddPartition(partition)) {
      return 0;
    }
    int partitionID = partition.getPartitionID();
    P oldPartition = this.partitions.get(partitionID);
    if (oldPartition == null) {
      this.partitions.put(partitionID, partition);
      return 1;
    } else {
      // Try to merge two partitions
      mergePartition(oldPartition, partition);
      return 2;
    }
  }
  
  protected abstract boolean checkAddPartition(P p);

  protected abstract void mergePartition(P op, P np);

  public abstract P[] getPartitions();
  
  public P getPartition(int partitionID) {
    return this.partitions.get(partitionID);
  }
}
