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
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.comm.data.StructObject;

/**
 * Each partition could have multi-destinations.
 * 
 * @author zhangbj
 *
 */
public class MultiBinAllToAllReq extends StructObject {
  /** Which worker this partition goes */
  private Int2ObjectOpenHashMap<IntArrayList> partitionWorkerMap;
  /** How many Data arrays this worker needs to receive */
  private Int2IntOpenHashMap workerPartitionCountMap;

  public MultiBinAllToAllReq() {
  }

  public MultiBinAllToAllReq(Int2ObjectOpenHashMap<IntArrayList> partitionWorkersMap,
    Int2IntOpenHashMap workerPartitionCountMap) {
    this.partitionWorkerMap = partitionWorkersMap;
    this.workerPartitionCountMap = workerPartitionCountMap;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(partitionWorkerMap.size());
    for (Int2ObjectMap.Entry<IntArrayList> entry : partitionWorkerMap
      .int2ObjectEntrySet()) {
      out.writeInt(entry.getIntKey());
      out.writeInt(entry.getValue().size());
      for (int workerID : entry.getValue()) {
        out.writeInt(workerID);
      }
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
    partitionWorkerMap = new Int2ObjectOpenHashMap<IntArrayList>(mapSize);
    int key;
    int listSize;
    IntArrayList list;
    for (int i = 0; i < mapSize; i++) {
      key = in.readInt();
      listSize = in.readInt();
      list = new IntArrayList(listSize);
      for (int j = 0; j < listSize; j++) {
        list.add(in.readInt());
      }
      partitionWorkerMap.put(key, list);
    }
    mapSize = in.readInt();
    workerPartitionCountMap = new Int2IntOpenHashMap(mapSize);
    for (int i = 0; i < mapSize; i++) {
      workerPartitionCountMap.put(in.readInt(), in.readInt());
    }
  }

  public Int2ObjectOpenHashMap<IntArrayList> getPartitionWorkerMap() {
    return partitionWorkerMap;
  }

  public Int2IntOpenHashMap getWorkerPartitionCountMap() {
    return workerPartitionCountMap;
  }

  @Override
  public int getSizeInBytes() {
    int size = 4;
    for (Int2ObjectMap.Entry<IntArrayList> entry : partitionWorkerMap
      .int2ObjectEntrySet()) {
      size = size + 4 + 4 + entry.getValue().size() * 4;
    }
    size = size + 4 + this.workerPartitionCountMap.size() * 8;
    return size;
  }
}
