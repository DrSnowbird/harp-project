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

package edu.iu.pagerank;

import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.graph.EdgePartition;
import edu.iu.harp.graph.LongVertexID;
import edu.iu.harp.graph.NullEdgeVal;
import edu.iu.harp.graph.vtx.LongDblVtxPartition;

/**
 * Build Vertex partition through in-edge partition.
 * 
 * @author zhangbj
 * 
 */
public class InitValTask1 extends
  Task<EdgePartition<LongVertexID, NullEdgeVal>, LongDblVtxPartition> {

  private int expVtxCount;
  private int totalVtxCount;
  private ResourcePool resourcePool;

  public InitValTask1(int expVtxCount, int totalVtxCount,
    ResourcePool resourcePool) {
    this.expVtxCount = expVtxCount;
    this.totalVtxCount = totalVtxCount;
    this.resourcePool = resourcePool;
  }

  @Override
  public LongDblVtxPartition run(
    EdgePartition<LongVertexID, NullEdgeVal> edgePartition) throws Exception {
    LongDblVtxPartition vtxPartition = (LongDblVtxPartition) resourcePool
      .getWritableObjectPool().getWritableObject(
        LongDblVtxPartition.class.getName());
    vtxPartition.initialize(edgePartition.getPartitionID(), expVtxCount);
    LongVertexID targetID = null;
    double initVal = (double) 1 / (double) totalVtxCount;
    while (edgePartition.nextEdge()) {
      // From in-edge table, generate vtx table
      // partition distribution of these two tables should be matched
      targetID = edgePartition.getCurTargetID();
      vtxPartition.putVertexVal(targetID.getVertexID(), initVal);
    }
    edgePartition.defaultReadPos();
    return vtxPartition;
  }
}
