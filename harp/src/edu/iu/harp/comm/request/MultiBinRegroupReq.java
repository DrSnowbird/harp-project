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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.comm.data.StructObject;

/**
 * Each partition only has one destination.
 * 
 * @author zhangbj
 *
 */
public class MultiBinRegroupReq extends StructObject {
  /** Which worker this partition goes */
  private Int2IntOpenHashMap partitionWorkerMap;
  /** How many Data arrays this worker needs to receive */
  private Int2IntOpenHashMap workerPartitionCountMap;

  public MultiBinRegroupReq() {
  }

  public MultiBinRegroupReq(Int2IntOpenHashMap partitionWorkersMap,
    Int2IntOpenHashMap workerPartitionCountMap) {
    this.partitionWorkerMap = partitionWorkersMap;
    this.workerPartitionCountMap = workerPartitionCountMap;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(partitionWorkerMap.size());
    for (Int2IntMap.Entry entry : partitionWorkerMap.int2IntEntrySet()) {
      out.writeInt(entry.getIntKey());
      out.writeInt(entry.getIntValue());
    }
    out.writeInt(this.workerPartitionCountMap.size());
    for (Int2IntMap.Entry entry : this.workerPartitionCountMap
      .int2IntEntrySet()) {
      out.writeInt(entry.getIntKey());
      out.writeInt(entry.getIntValue());
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    int mapSize = in.readInt();
    partitionWorkerMap = new Int2IntOpenHashMap(mapSize);
    int key;
    int val;
    for (int i = 0; i < mapSize; i++) {
      key = in.readInt();
      val = in.readInt();
      partitionWorkerMap.put(key, val);
    }
    mapSize = in.readInt();
    workerPartitionCountMap = new Int2IntOpenHashMap(mapSize);
    for (int i = 0; i < mapSize; i++) {
      workerPartitionCountMap.put(in.readInt(), in.readInt());
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
    int size = 4;
    size = size + 8 * partitionWorkerMap.size();
    size = size + 4 + this.workerPartitionCountMap.size() * 8;
    return size;
  }
}
