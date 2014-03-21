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

package edu.iu.harp.test;

import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.graph.EdgeTable;
import edu.iu.harp.graph.EdgePartition;
import edu.iu.harp.graph.InEdgeTable;
import edu.iu.harp.graph.LongVertexID;
import edu.iu.harp.graph.NullEdgeVal;

public class GraphTest {

  public static void main(String[] args) {
    ResourcePool pool = new ResourcePool();
    EdgeTable<LongVertexID, NullEdgeVal> edgeTable = new InEdgeTable<LongVertexID, NullEdgeVal>(
      0, 5, 100, LongVertexID.class, NullEdgeVal.class, pool);
    edgeTable.addEdge(new LongVertexID(1), new NullEdgeVal(), new LongVertexID(
      2));
    edgeTable.addEdge(new LongVertexID(3), new NullEdgeVal(), new LongVertexID(
      4));

    for (EdgePartition<LongVertexID, NullEdgeVal> partition : edgeTable
      .getPartitions()) {
      while (partition.nextEdge()) {
        System.out.println("Partition: " + partition.getPartitionID() + " "
          + partition.getCurSourceID().getVertexID() + " "
          + partition.getCurTargetID().getVertexID());
      }
    }
    
    System.out.println("Edge partition test.");
    EdgePartition<LongVertexID, NullEdgeVal> edgePartition =
      new EdgePartition<LongVertexID, NullEdgeVal>(
      0, pool, LongVertexID.class, NullEdgeVal.class, 1000);
    edgePartition.addEdge(new LongVertexID(1), new NullEdgeVal(), new LongVertexID(
      2));
    edgePartition.addEdge(new LongVertexID(3), new NullEdgeVal(), new LongVertexID(
      4));
    edgePartition.addEdge(new LongVertexID(9), new NullEdgeVal(), new LongVertexID(
      10));
    
    while (edgePartition.nextEdge()) {
      if(edgePartition.getCurSourceID().getVertexID() == 3) {
        // edgePartition.getCurrentTargetID().setVertexID(7);
        // edgePartition.saveCurrentEdge();
        edgePartition.removeCurEdge();
      }
    }
    edgePartition.addEdge(new LongVertexID(11), new NullEdgeVal(), new LongVertexID(
      12));
    
    edgePartition.defaultReadPos();

    while (edgePartition.nextEdge()) {

      System.out.println("Partition: " + edgePartition.getPartitionID() + " "
        + edgePartition.getCurSourceID().getVertexID() + " "
        + edgePartition.getCurTargetID().getVertexID());

    }
  }
}
