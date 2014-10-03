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

import edu.iu.harp.util.Int2ObjectReuseHashMap;

/**
 * This vertex table is for int vertex id and float array vertex value
 * 
 * @author zhangbj
 */
public class IntFltArrVtxTable extends StructTable<IntFltArrVtxPartition> {

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(IntFltArrVtxTable.class);

  protected int maxNumPartitions;
  protected int parExpSize;
  protected int arrLen;

  public IntFltArrVtxTable(int tableID, int maxNumPartitions, int parExpSize,
    int arrLen) {
    super(tableID);
    this.maxNumPartitions = maxNumPartitions;
    this.parExpSize = parExpSize;
    this.arrLen = arrLen;
  }

  public int getArrLen() {
    return this.arrLen;
  }

  public IntFltArrVtxPartition[] getPartitions() {
    IntFltArrVtxPartition[] parArray = new IntFltArrVtxPartition[partitions
      .size()];
    return partitions.values().toArray(parArray);
  }

  public boolean initVertexVal(int vertexID, float[] vertexVal) {
    IntFltArrVtxPartition partition = getOrCreatePartition(vertexID);
    return partition.initVertexVal(vertexID, vertexVal);
  }

  public boolean addVertexVal(int vertexID, float[] vertexVal) {
    IntFltArrVtxPartition partition = getOrCreatePartition(vertexID);
    return partition.addVertexVal(vertexID, vertexVal);
  }

  public boolean putVertexVal(int vertexID, float[] vertexVal) {
    IntFltArrVtxPartition partition = getOrCreatePartition(vertexID);
    return partition.putVertexVal(vertexID, vertexVal);
  }

  public float[] getVertexVal(int vertexID) {
    IntFltArrVtxPartition partition = getPartition(vertexID);
    if (partition != null) {
      return partition.getVertexVal(vertexID);
    } else {
      return null;
    }
  }

  private IntFltArrVtxPartition getOrCreatePartition(int vertexID) {
    int partitionID = getVertexPartitionID(vertexID);
    IntFltArrVtxPartition partition = partitions.get(partitionID);
    if (partition == null) {
      partition = new IntFltArrVtxPartition(partitionID, parExpSize, arrLen);
      partitions.put(partitionID, partition);
    }
    return partition;
  }

  public IntFltArrVtxPartition getPartition(int vertexID) {
    int partitionID = getVertexPartitionID(vertexID);
    IntFltArrVtxPartition partition = partitions.get(partitionID);
    if (partition == null) {
      return null;
    }
    return partition;
  }

  private int getVertexPartitionID(long vertexID) {
    return (int) (vertexID % this.maxNumPartitions);
  }

  @Override
  protected boolean checkAddPartition(IntFltArrVtxPartition p) {
    if (p.getArrayLength() != this.arrLen) {
      LOG.info("Partition doesn't match to the definition of the table.");
      return false;
    }
    return true;
  }

  @Override
  protected void mergePartition(IntFltArrVtxPartition op,
    IntFltArrVtxPartition np) {
    Int2ObjectReuseHashMap<float[]> nMap = np.getVertexMap();
    for (Int2ObjectReuseHashMap.Entry<float[]> entry : nMap
      .int2ObjectEntrySet()) {
      op.addVertexVal(entry.getIntKey(), entry.getValue());
    }
  }
}
