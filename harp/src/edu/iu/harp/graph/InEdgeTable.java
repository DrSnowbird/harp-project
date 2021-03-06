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

import edu.iu.harp.comm.resource.ResourcePool;

public class InEdgeTable<I extends VertexID, E extends EdgeVal> extends
  EdgeTable<I, E> {

  public InEdgeTable(int tableID, int numPartitions, int parExpectedSize,
    Class<I> iClass, Class<E> eClass, ResourcePool pool) {
    super(tableID, numPartitions, parExpectedSize, iClass, eClass, pool);
  }

  @Override
  public int getEdgePartitionID(I sourceID, I targetID) {
    return Math.abs(targetID.hashCode()) % this.getMaxNumPartitions();
  }
}