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

import java.util.List;

import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.resource.ResourcePool;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;

public abstract class EdgeTable<I extends VertexID, E extends EdgeVal> {

  private int tableID;
  private int maxNumPartitions;
  private Int2ObjectAVLTreeMap<EdgePartition<I, E>> partitions;
  private int parExpectedSize;
  private ResourcePool pool;
  private Class<I> iClass;
  private Class<E> eClass;

  public EdgeTable(int tableID, int maxNumPartitions, int parExpectedSize, Class<I> iClass,
    Class<E> eClass,  ResourcePool pool) {
    this.tableID = tableID;
    this.maxNumPartitions = maxNumPartitions;
    this.partitions = new Int2ObjectAVLTreeMap<EdgePartition<I, E>>();
    this.parExpectedSize = parExpectedSize;
    this.iClass = iClass;
    this.eClass = eClass;
    this.pool = pool;
  }

  public int getTableID() {
    return tableID;
  }

  public int[] getPartitionIDs() {
    return partitions.keySet().toIntArray();
  }

  public EdgePartition<I, E>[] getPartitions() {
    EdgePartition<I, E>[] parArray = new EdgePartition[this.partitions.size()];
    return this.partitions.values().toArray(parArray);
  }
  
  public int getNumPartitions() {
    return this.partitions.size();
  }
  
  public int getMaxNumPartitions() {
    return this.maxNumPartitions;
  }

  public void addEdge(I sourceID, E edgeVal, I targetID) {
    int partitionID = getEdgePartitionID(sourceID, targetID);
    EdgePartition<I, E> partition = getEdgePartition(partitionID);
    partition.addEdge(sourceID, edgeVal, targetID);
  }

  public void addData(int partitionID, ByteArray array) {
    EdgePartition<I, E> partition = getEdgePartition(partitionID);
    partition.addData(array);
  }

  public void addAllData(int partitionID, List<ByteArray> arrays) {
    EdgePartition<I, E> partition = getEdgePartition(partitionID);
    partition.addAllData(arrays);
  }

  private EdgePartition<I, E> getEdgePartition(int partitionID) {
    EdgePartition<I, E> partition = this.partitions.get(partitionID);
    if (partition == null) {
      partition = new EdgePartition<I, E>(partitionID, pool, iClass, eClass,
        this.parExpectedSize);
      this.partitions.put(partitionID, partition);
    }
    return partition;
  }

  public void removePartition(int partitionID) {
    partitions.remove(partitionID);
  }

  public abstract int getEdgePartitionID(I sourceID, I targetID);
}
