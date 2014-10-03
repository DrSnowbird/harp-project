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

import org.apache.log4j.Logger;

import edu.iu.harp.util.Long2ObjectReuseHashMap;

/**
 * This vertex table is for pagerank value
 * 
 * @author zhangbj
 */
public class LongDblArrVtxTable extends StructTable<LongDblArrVtxPartition> {

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(LongDblArrVtxTable.class);

  protected int maxNumPartitions;
  protected int parExpSize;
  protected int arrLen;

  public LongDblArrVtxTable(int tableID, int maxNumPartitions, int parExpSize,
    int arrLen) {
    super(tableID);
    this.maxNumPartitions = maxNumPartitions;
    this.parExpSize = parExpSize;
    this.arrLen = arrLen;
  }

  public int getArrLen() {
    return this.arrLen;
  }

  public LongDblArrVtxPartition[] getPartitions() {
    LongDblArrVtxPartition[] parArray = new LongDblArrVtxPartition[this.partitions
      .size()];
    return this.partitions.values().toArray(parArray);
  }

  public boolean initVertexVal(long vertexID, double[] vertexVal) {
    LongDblArrVtxPartition partition = getOrCreatePartition(vertexID);
    return partition.initVertexVal(vertexID, vertexVal);
  }

  public boolean addVertexVal(long vertexID, double[] vertexVal) {
    LongDblArrVtxPartition partition = getOrCreatePartition(vertexID);
    return partition.addVertexVal(vertexID, vertexVal);
  }

  public boolean putVertexVal(long vertexID, double[] vertexVal) {
    LongDblArrVtxPartition partition = getOrCreatePartition(vertexID);
    return partition.putVertexVal(vertexID, vertexVal);
  }

  public double[] getVertexVal(long vertexID) {
    LongDblArrVtxPartition partition = getPartition(vertexID);
    if (partition != null) {
      return partition.getVertexVal(vertexID);
    } else {
      return null;
    }
  }

  private LongDblArrVtxPartition getOrCreatePartition(long vertexID) {
    int partitionID = getVertexPartitionID(vertexID);
    LongDblArrVtxPartition partition = this.partitions.get(partitionID);
    if (partition == null) {
      partition = new LongDblArrVtxPartition(partitionID, this.parExpSize,
        this.arrLen);
      this.partitions.put(partitionID, partition);
    }
    return partition;
  }

  public LongDblArrVtxPartition getPartition(long vertexID) {
    int partitionID = getVertexPartitionID(vertexID);
    LongDblArrVtxPartition partition = this.partitions.get(partitionID);
    if (partition == null) {
      return null;
    }
    return partition;
  }

  private int getVertexPartitionID(long vertexID) {
    return (int) (vertexID % this.maxNumPartitions);
  }

  @Override
  protected boolean checkAddPartition(LongDblArrVtxPartition p) {
    if (p.getArrayLength() != this.arrLen) {
      LOG.info("Partition doesn't match to the definition of the table.");
      return false;
    }
    return true;
  }

  @Override
  protected void mergePartition(LongDblArrVtxPartition op,
    LongDblArrVtxPartition np) {
    Long2ObjectReuseHashMap<double[]> nMap = np.getVertexMap();
    for (Long2ObjectReuseHashMap.Entry<double[]> entry : nMap
      .long2ObjectEntrySet()) {
      op.addVertexVal(entry.getLongKey(), entry.getValue());
    }
  }
}
