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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

import edu.iu.harp.comm.data.StructObject;

public class MultiBinParGenAck extends StructObject {
  private int workerID;
  // <partition, byte array count>
  private Int2IntOpenHashMap partitionDataCount;

  public MultiBinParGenAck() {
  }

  public MultiBinParGenAck(int workerID, Int2IntOpenHashMap partitionIds) {
    this.workerID = workerID;
    this.partitionDataCount = partitionIds;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(workerID);
    out.writeInt(partitionDataCount.size());
    for (Entry<Integer, Integer> entry : partitionDataCount.entrySet()) {
      out.writeInt(entry.getKey());
      out.writeInt(entry.getValue());
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    this.workerID = in.readInt();
    int size = in.readInt();
    partitionDataCount = new Int2IntOpenHashMap(size);
    for (int i = 0; i < size; i++) {
      partitionDataCount.put(in.readInt(), in.readInt());
    }
  }

  public int getWorkerID() {
    return workerID;
  }

  public Int2IntOpenHashMap getParDataCount() {
    return partitionDataCount;
  }

  @Override
  public int getSizeInBytes() {
    return 4 + 4 + partitionDataCount.size() * 8;
  }
}
