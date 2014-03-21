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

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import edu.iu.harp.comm.data.StructObject;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.graph.vtx.StructPartition;

public class MultiStructPartition<P extends StructPartition> extends
  StructObject {

  private List<P> partitionList;
  private ResourcePool resourcePool;

  public MultiStructPartition() {
    this.partitionList = new ObjectArrayList<P>();
  }

  public MultiStructPartition(List<P> partitionList, ResourcePool pool) {
    this.partitionList = partitionList;
    this.resourcePool = pool;
  }

  public void setResourcePool(ResourcePool pool) {
    this.resourcePool = pool;
  }

  public List<P> getPartitionList() {
    return this.partitionList;
  }

  @Override
  public int getSizeInBytes() {
    int size = 4;
    if (this.partitionList.size() > 0) {
      // partition class name and its name length
      size = size + 4 + partitionList.get(0).getClass().getName().length() * 2;
    }
    for (P partition : this.partitionList) {
      size = size + partition.getSizeInBytes();
    }
    return size;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(partitionList.size());
    if (partitionList.size() > 0) {
      out.writeUTF(partitionList.get(0).getClass().getName());
      for (P partition : partitionList) {
        partition.write(out);
      }
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    int size = in.readInt();
    if (size > 0) {
      String className = in.readUTF();
      P partition = null;
      for (int i = 0; i < size; i++) {
        try {
          partition = (P) resourcePool.getWritableObjectPool()
            .getWritableObject(className);
          partition.read(in);
          this.partitionList.add(partition);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
  }
}
