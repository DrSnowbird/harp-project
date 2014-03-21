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

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

/**
 * This vertex table is for pagerank value
 * 
 * @author zhangbj
 */
public class LongDblVtxTable extends StructTable<LongDblVtxPartition> {
  protected int maxNumPartitions;
  protected int parExpSize;

  public LongDblVtxTable(int tableID, int maxNumPartitions, int parExpSize) {
    super(tableID);
    this.maxNumPartitions = maxNumPartitions;
    this.parExpSize = parExpSize;

  }

  public LongDblVtxPartition[] getPartitions() {
    LongDblVtxPartition[] parArray = new LongDblVtxPartition[this.partitions
      .size()];
    return this.partitions.values().toArray(parArray);
  }

  public void addVertexVal(long vertexID, double vertexVal) {
    LongDblVtxPartition partition = getPartition(vertexID);
    partition.addVertexVal(vertexID, vertexVal);
  }

  public void putVertexVal(long vertexID, double vertexVal) {
    LongDblVtxPartition partition = getPartition(vertexID);
    partition.putVertexVal(vertexID, vertexVal);
  }

  public double getVertexVal(long vertexID) {
    LongDblVtxPartition partition = getPartition(vertexID);
    return partition.getVertexVal(vertexID);
  }

  private LongDblVtxPartition getPartition(long vertexID) {
    int partitionID = getVertexPartitionID(vertexID);
    LongDblVtxPartition partition = this.partitions.get(partitionID);
    if (partition == null) {
      partition = new LongDblVtxPartition(partitionID, this.parExpSize);
      this.partitions.put(partitionID, partition);
    }
    return partition;
  }

  private int getVertexPartitionID(long vertexID) {
    return (int) (vertexID % this.maxNumPartitions);
  }

  @Override
  protected boolean checkAddPartition(LongDblVtxPartition p) {
    return true;
  }

  @Override
  protected void mergePartition(LongDblVtxPartition op, LongDblVtxPartition np) {
    Long2DoubleOpenHashMap oMap = op.getVertexMap();
    Long2DoubleOpenHashMap nMap = np.getVertexMap();
    for (Long2DoubleMap.Entry entry : nMap.long2DoubleEntrySet()) {
      if (oMap.containsKey(entry.getLongKey())) {
        oMap.addTo(entry.getLongKey(), entry.getDoubleValue());
      } else {
        oMap.put(entry.getLongKey(), entry.getDoubleValue());
      }
    }
  }
}
