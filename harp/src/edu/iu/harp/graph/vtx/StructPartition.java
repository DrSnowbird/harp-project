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

import edu.iu.harp.arrpar.Partition;
import edu.iu.harp.comm.data.StructObject;

public abstract class StructPartition extends StructObject implements Partition {
  private int partitionID;

  public StructPartition() {
  }

  public StructPartition(int partitionID) {
    this.partitionID = partitionID;
  }

  @Override
  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }

  @Override
  public int getPartitionID() {
    return this.partitionID;
  }
}
