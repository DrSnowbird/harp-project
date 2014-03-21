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
import edu.iu.harp.graph.vtx.LongDblVtxTable;

public class CalcPRTask1 extends
  Task<EdgePartition<LongVertexID, NullEdgeVal>, LongDblVtxPartition> {

  private LongDblVtxTable prValTable;
  private int expVtxCount;
  private ResourcePool resourcePool;

  public CalcPRTask1(LongDblVtxTable prValTable, int expVtxCount,
    ResourcePool resourcePool) {
    this.prValTable = prValTable;
    this.expVtxCount = expVtxCount;
    this.resourcePool = resourcePool;
  }

  @Override
  public LongDblVtxPartition run(
    EdgePartition<LongVertexID, NullEdgeVal> edgePartition) throws Exception {
    LongDblVtxPartition ldVtxPartition = (LongDblVtxPartition) resourcePool
      .getWritableObjectPool().getWritableObject(
        LongDblVtxPartition.class.getName());
    ldVtxPartition.initialize(edgePartition.getPartitionID(), expVtxCount);
    LongVertexID sourceID = null;
    LongVertexID targetID = null;
    while (edgePartition.nextEdge()) {
      sourceID = edgePartition.getCurSourceID();
      targetID = edgePartition.getCurTargetID();
      ldVtxPartition.addVertexVal(targetID.getVertexID(),
        prValTable.getVertexVal(sourceID.getVertexID()));
    }
    edgePartition.defaultReadPos();
    return ldVtxPartition;
  }
}
