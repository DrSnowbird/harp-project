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

import edu.iu.harp.util.Long2ObjectOpenHashMap;

public abstract class LongObjVtxPartition<V> extends
  StructPartition {

  // We use a customized Long2ObjectOpenHashMap from fastutil
  private Long2ObjectOpenHashMap<V> vertexMap;

  public LongObjVtxPartition() {  
  }
  
  public void initialize(int partitionID, int expVtxCount, Class<V> vClass) {
    this.setPartitionID(partitionID);
    if (this.vertexMap != null) {
      this.vertexMap.clean();
    } else {
      this.vertexMap = new Long2ObjectOpenHashMap<V>(expVtxCount, vClass);
      this.vertexMap.defaultReturnValue(null);
    }
  }
  
  public LongObjVtxPartition(int partitionID, int expVtxCount,
    Class<V> vClass) {
    super(partitionID);
    this.vertexMap = new Long2ObjectOpenHashMap<V>(expVtxCount, vClass);
    this.vertexMap.defaultReturnValue(null);
  }
  
  public abstract boolean initVertexVal(long vertexID, V vertexVal);

  /**
   * If object V is a primitive array, copy the elements in vertexVal into the
   * partition, or create an array to hold the elements. In this way, the
   * vertexVal in parameter can be used for other purpose. If V is an object,
   * add it to partition directly.
   * 
   * @param vertexID
   * @param vertexVal
   */
  public abstract boolean addVertexVal(long vertexID, V vertexVal);

  public abstract boolean putVertexVal(long vertexID, V vertexVal);

  public V getVertexVal(long vertexID) {
    return this.vertexMap.get(vertexID);
  }

  public int size() {
    return this.vertexMap.size();
  }

  public boolean isEmpty() {
    return this.vertexMap.isEmpty();
  }

  public Long2ObjectOpenHashMap<V> getVertexMap() {
    return vertexMap;
  }

  protected void createVertexMap(int expVtxCount, Class<V> vClass) {
    this.vertexMap = new Long2ObjectOpenHashMap<V>(expVtxCount, vClass);
    this.vertexMap.defaultReturnValue(null);
  }
}
