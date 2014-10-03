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

package edu.iu.harp.arrpar;

import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.data.Array;

/**
 * Array table, which includes array partitions. When there is array partition
 * ID conflicts, use array combiner to combine two array partitions.
 * 
 * @author zhangbj
 * 
 * @param <A> Array type
 * @param <C> Array combiner type
 */
public class ArrTable<A extends Array<?>, C extends ArrCombiner<A>> implements
  Table {

  private static final Logger LOG = Logger.getLogger(ArrTable.class);

  private final int tableID;
  private Int2ObjectAVLTreeMap<ArrPartition<A>> partitions;
  private Class<A> aClass;
  private Class<C> cClass;
  private C combiner;

  public ArrTable(int tableID, Class<A> aClass, Class<C> cClass) {
    this.tableID = tableID;
    this.partitions = new Int2ObjectAVLTreeMap<ArrPartition<A>>();
    this.partitions.defaultReturnValue(null);
    this.aClass = aClass;
    setCombiner(cClass);
  }

  @Override
  public int getTableID() {
    return tableID;
  }

  public int[] getPartitionIDs() {
    return this.partitions.keySet().toIntArray();
  }

  @SuppressWarnings("unchecked")
  public ArrPartition<A>[] getPartitions() {
    ArrPartition<A>[] parArray = new ArrPartition[this.partitions.size()];
    return this.partitions.values().toArray(parArray);
  }
  
  public int getNumPartitions() {
    return this.partitions.size();
  }

  /**
   * addPartition, check if combining happens.
   * 
   * @param partition
   * @return True if combine happens.
   * @throws Exception
   */
  public boolean addPartition(ArrPartition<A> partition) throws Exception {
    int partitionID = partition.getPartitionID();
    ArrPartition<A> curPartition = this.partitions.get(partitionID);
    if (curPartition == null) {
      this.partitions.put(partitionID, partition);
      return false;
    } else {
      // Try to combine on current partition
      combiner.combine(curPartition, partition);
      return true;
    }
  }
  
  public ArrPartition<A> getPartition(int partitionID) {
    return this.partitions.get(partitionID);
  }

  public ArrPartition<A> removePartition(int partitionID) {
    return this.partitions.remove(partitionID);
  }

  public boolean isEmpty() {
    return this.partitions.isEmpty();
  }

  private void setCombiner(Class<C> cClass) {
    try {
      this.cClass = cClass;
      this.combiner = cClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      LOG.error("Fail to initialize partition combiner.", e);
    }
  }

  public C getCombiner() {
    return this.combiner;
  }

  public Class<A> getAClass() {
    return this.aClass;
  }

  public Class<C> getCClass() {
    return this.cClass;
  }
}
