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
import java.util.Arrays;

import edu.iu.harp.comm.data.StructObject;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.graph.vtx.StructPartition;

public class MultiStructPartition extends StructObject {

  private StructPartition[] partitions;
  private int numPartitions;
  private ResourcePool resourcePool;

  public MultiStructPartition() {
  }

  public MultiStructPartition(StructPartition[] partitions, ResourcePool pool) {
    this.partitions = partitions;
    numPartitions = 0;
    for (StructPartition partition : partitions) {
      if (partition != null) {
        numPartitions++;
      }
    }
    this.resourcePool = pool;
  }

  public void setResourcePool(ResourcePool pool) {
    this.resourcePool = pool;
  }

  public void setPartitions(StructPartition[] partitions) {
    this.partitions = partitions;
    numPartitions = 0;
    for (StructPartition partition : partitions) {
      if (partition != null) {
        numPartitions++;
      }
    }
  }

  public StructPartition[] getPartitions() {
    return partitions;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public void clean() {
    Arrays.fill(partitions, null);
    numPartitions = 0;
    resourcePool = null;
  }

  @Override
  public int getSizeInBytes() {
    int size = 4;
    if (numPartitions > 0) {
      // partition class name and its name length
      size = size + 4
        + partitions.getClass().getComponentType().getName().length() * 2;
      for (StructPartition partition : partitions) {
        if (partition != null) {
          size += partition.getSizeInBytes();
        }
      }
    }
    return size;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(numPartitions);
    if (numPartitions > 0) {
      // Try to get the real class name of the struct-partitions
      out.writeUTF(partitions[0].getClass().getName());
      for (StructPartition partition : partitions) {
        if (partition != null) {
          partition.write(out);
        }
      }
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    numPartitions = in.readInt();
    if (numPartitions > 0) {
      if (partitions == null) {
        partitions = new StructPartition[numPartitions];
      } else if (partitions.length < numPartitions) {
        partitions = new StructPartition[numPartitions];
      }
      String className = in.readUTF();
      StructPartition partition = null;
      for (int i = 0; i < numPartitions; i++) {
        try {
          partition = (StructPartition) resourcePool.getWritableObjectPool()
            .getWritableObject(className);
          partition.read(in);
          partitions[i] = partition;
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
  }
}
