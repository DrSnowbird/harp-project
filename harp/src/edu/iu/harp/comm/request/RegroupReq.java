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

package edu.iu.harp.comm.request;

import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.comm.data.StructObject;

public class RegroupReq extends StructObject {
  /** Which worker this partition goes */
  private Int2IntOpenHashMap partitionWorkerMap;
  /** How many partitions this worker needs to receive */
  private Int2IntOpenHashMap workerPartitionCountMap;

  public RegroupReq() {
  }

  public RegroupReq(Int2IntOpenHashMap partitionWorkersMap,
    Int2IntOpenHashMap workerPartitionCountMap) {
    this.partitionWorkerMap = partitionWorkersMap;
    this.workerPartitionCountMap = workerPartitionCountMap;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.partitionWorkerMap.size());
    for (Entry entry : this.partitionWorkerMap.int2IntEntrySet()) {
      out.writeInt(entry.getIntKey());
      out.writeInt(entry.getIntValue());
    }
    out.writeInt(this.workerPartitionCountMap.size());
    for (Entry entry : this.workerPartitionCountMap.int2IntEntrySet()) {
      out.writeInt(entry.getIntKey());
      out.writeInt(entry.getIntValue());
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    int mapSize = in.readInt();
    partitionWorkerMap = new Int2IntOpenHashMap();
    for (int i = 0; i < mapSize; i++) {
      this.partitionWorkerMap.put(in.readInt(), in.readInt());
    }
    mapSize = in.readInt();
    workerPartitionCountMap = new Int2IntOpenHashMap();
    for (int i = 0; i < mapSize; i++) {
      this.workerPartitionCountMap.put(in.readInt(), in.readInt());
    }
  }

  public Int2IntOpenHashMap getPartitionWorkerMap() {
    return partitionWorkerMap;
  }

  public Int2IntOpenHashMap getWorkerPartitionCountMap() {
    return workerPartitionCountMap;
  }

  @Override
  public int getSizeInBytes() {
    return 4 + this.partitionWorkerMap.size() * 8 + 4
      + this.workerPartitionCountMap.size() * 8;
  }
}
