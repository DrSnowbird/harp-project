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

import edu.iu.harp.comm.data.Array;

/**
 * Array partition is extended from Array. It includes an array and a partition
 * ID.
 * 
 * @author zhangbj
 * 
 * @param <T> Array type
 */
public class ArrPartition<T extends Array<?>> implements Partition {
  private int partitionID;
  private T array;

  public ArrPartition(T array, int partitionID) {
    this.array = array;
    this.partitionID = partitionID;
  }

  public int getPartitionID() {
    return partitionID;
  }

  public T getArray() {
    return this.array;
  }

  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }
}
