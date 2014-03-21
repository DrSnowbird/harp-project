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

import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class MsgTable<I extends VertexID, M extends MsgVal> {
  private int tableID;
  private int numPartitions;
  private Int2ObjectAVLTreeMap<MsgPartition<I, M>> partitions;
  private int parExpectedSize;
  private ResourcePool pool;
  private Class<I> iClass;
  private Class<M> mClass;

  public MsgTable(int tableID, int numPartitions, int parExpectedSize,
    Class<I> iClass, Class<M> eClass, ResourcePool pool) {
    this.tableID = tableID;
    this.numPartitions = numPartitions;
    this.partitions = new Int2ObjectAVLTreeMap<MsgPartition<I, M>>();
    this.parExpectedSize = parExpectedSize;
    this.iClass = iClass;
    this.mClass = eClass;
    this.pool = pool;
  }

  public int getTableID() {
    return tableID;
  }

  public int[] getPartitionIDs() {
    return partitions.keySet().toIntArray();
  }

  public MsgPartition<I, M>[] getPartitions() {
    MsgPartition<I, M>[] parArray = new MsgPartition[this.partitions.size()];
    return this.partitions.values().toArray(parArray);
  }

  public int getNumPartitions() {
    return this.numPartitions;
  }

  public void addMsg(I vertexID, M msgVal) {
    MsgPartition<I, M> partition = getMsgPartition(getMsgPartitionID(vertexID));
    partition.addMsg(vertexID, msgVal);
  }

  public void addData(int partitionID, ByteArray array) {
    MsgPartition<I, M> partition = getMsgPartition(partitionID);
    partition.addData(array);
  }

  /**
   * When data added, read position are reset to 0.
   *
   * @param partitionID
   * @param arrays
   */
  public void addAllData(int partitionID, List<ByteArray> arrays) {
    MsgPartition<I, M> partition = getMsgPartition(partitionID);
    partition.addAllData(arrays);
  }
  
  private MsgPartition<I, M> getMsgPartition(int partitionID) {
    MsgPartition<I, M> partition = this.partitions.get(partitionID);
    if (partition == null) {
      partition = new MsgPartition<I, M>(partitionID, pool, iClass, mClass,
        this.parExpectedSize);
      this.partitions.put(partitionID, partition);
    }
    return partition;
  }

  public int getMsgPartitionID(I vertexID) {
    return vertexID.hashCode() % this.numPartitions;
  }

  public void removePartition(int partitionID) {
    partitions.remove(partitionID);
  }
}
