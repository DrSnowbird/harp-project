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

import it.unimi.dsi.fastutil.longs.Long2IntMap;

import org.apache.log4j.Logger;

import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.graph.vtx.LongDblVtxPartition;
import edu.iu.harp.graph.vtx.LongDblVtxTable;
import edu.iu.harp.graph.vtx.LongIntVtxPartition;

class Null {
}

public class CalcPRTask2 extends Task<LongIntVtxPartition, Null> {

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(CalcPRTask2.class);

  private LongDblVtxTable prValTable;
  private int expVtxCount;
  private int totalVtxCount;
  private ResourcePool resourcePool;

  public CalcPRTask2(LongDblVtxTable prValTable, int expVtxCount,
    int totalVtxCount, ResourcePool resourcePool) {
    this.prValTable = prValTable;
    this.expVtxCount = expVtxCount;
    this.totalVtxCount = totalVtxCount;
    this.resourcePool = resourcePool;
  }

  @Override
  public Null run(LongIntVtxPartition liVtxPartition) throws Exception {
    int partitionID = liVtxPartition.getPartitionID();
    // Get matched partition from page-rank value vertex table
    LongDblVtxPartition ldVtxPartition = prValTable.getPartition(partitionID);
    boolean newPartition = false;
    if (ldVtxPartition == null) {
      ldVtxPartition = (LongDblVtxPartition) resourcePool
        .getWritableObjectPool().getWritableObject(
          LongDblVtxPartition.class.getName());
      ldVtxPartition.initialize(partitionID, expVtxCount);
      newPartition = true;
    }
    // Update page rank value which has out edges for next iteration
    // Since original page-rank value vertex table
    // is generated based on in-edges, if this vertex
    // doesn't have any out edges, its value will not be updated here
    long vertexID;
    int outEdgeCount;
    double prVal;
    double initVal = (double) 1 / (double) totalVtxCount;
    for (Long2IntMap.Entry entry : liVtxPartition.getVertexMap()
      .long2IntEntrySet()) {
      vertexID = entry.getLongKey();
      outEdgeCount = entry.getIntValue();
      prVal = ldVtxPartition.getVertexVal(vertexID);
      if (prVal != 0) {
        ldVtxPartition.putVertexVal(vertexID, prVal / outEdgeCount);
      } else {
        ldVtxPartition.putVertexVal(vertexID, initVal / outEdgeCount);
      }
    }
    // Add missed vertices
    if (newPartition) {
      synchronized (prValTable) {
        LOG.info("Add new partition: " + ldVtxPartition.getPartitionID());
        prValTable.addPartition(ldVtxPartition);
      }
    }
    return new Null();
  }
}
