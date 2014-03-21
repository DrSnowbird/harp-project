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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.comm.data.StructObject;

public class ParGenAck extends StructObject {
  private int workerID;
  private int[] partitionIds;

  public ParGenAck() {
  }

  public ParGenAck(int workerID, int[] partitionIds) {
    this.workerID = workerID;
    this.partitionIds = partitionIds;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(workerID);
    out.writeInt(partitionIds.length);
    for (int i = 0; i < partitionIds.length; i++) {
      out.writeInt(partitionIds[i]);
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    this.workerID = in.readInt();
    int size = in.readInt();
    int[] partitionIds = new int[size];
    for (int i = 0; i < size; i++) {
      partitionIds[i] = in.readInt();
    }
    this.partitionIds = partitionIds;
  }

  public int getWorkerID() {
    return workerID;
  }

  public int[] getPartitionIds() {
    return partitionIds;
  }

  @Override
  public int getSizeInBytes() {
    return 4 + 4 + partitionIds.length * 4;
  }
}
