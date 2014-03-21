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

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

public class LongIntVtxTable extends StructTable<LongIntVtxPartition> {
  protected int maxNumPartitions;
  protected int parExpSize;

  public LongIntVtxTable(int tableID, int maxNumPartitions, int parExpSize) {
    super(tableID);
    this.maxNumPartitions = maxNumPartitions;
    this.parExpSize = parExpSize;
  }

  public LongIntVtxPartition[] getPartitions() {
    LongIntVtxPartition[] parArray = new LongIntVtxPartition[this.partitions
      .size()];
    return this.partitions.values().toArray(parArray);
  }

  public void addVertexVal(long vertexID, int vertexVal) {
    LongIntVtxPartition partition = getPartition(vertexID);
    partition.addVertexVal(vertexID, vertexVal);
  }

  private LongIntVtxPartition getPartition(long vertexID) {
    int partitionID = getVertexPartitionID(vertexID);
    LongIntVtxPartition partition = this.partitions.get(partitionID);
    if (partition == null) {
      partition = new LongIntVtxPartition(partitionID, this.parExpSize);
      this.partitions.put(partitionID, partition);
    }
    return partition;
  }

  public int getVertexVal(long vertexID) {
    LongIntVtxPartition partition = getPartition(vertexID);
    return partition.getVertexVal(vertexID);
  }

  public int getVertexPartitionID(long vertexID) {
    return (int) (vertexID % this.maxNumPartitions);
  }

  @Override
  protected boolean checkAddPartition(LongIntVtxPartition p) {
    return true;
  }

  @Override
  protected void mergePartition(LongIntVtxPartition op, LongIntVtxPartition np) {
    Long2IntOpenHashMap oMap = op.getVertexMap();
    Long2IntOpenHashMap nMap = np.getVertexMap();
    for (Long2IntMap.Entry entry : nMap.long2IntEntrySet()) {
      if (oMap.containsKey(entry.getLongKey())) {
        oMap.addTo(entry.getLongKey(), entry.getIntValue());
      } else {
        oMap.put(entry.getLongKey(), entry.getIntValue());
      }
    }
  }
}
