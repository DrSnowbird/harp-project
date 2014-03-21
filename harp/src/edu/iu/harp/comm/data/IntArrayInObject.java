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

package edu.iu.harp.comm.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.log4j.Logger;

public class IntArrayInObject extends StructObject {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(IntArrayInObject.class);
  private int[] ints;
  private int size;

  public IntArrayInObject() {
  }

  public IntArrayInObject(int size) {
    LOG.info("Create a new int array with size " + size);
    this.ints = new int[size];
    this.size = size;
    ints[0] = 1;
    ints[this.size - 1] = 1;
  }

  public int getSize() {
    return this.size;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(size);
    for (int i = 0; i < size; i++) {
      out.writeInt(ints[i]);
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    size = in.readInt();
    if (ints == null || size > ints.length) {
      LOG.info("Create a new int array with size " + size);
      ints = new int[size];
    }
    for (int i = 0; i < size; i++) {
      ints[i] = in.readInt();
    }
    LOG.info("First element in the array : " + ints[0]);
    LOG.info("Last element in the array : " + ints[size - 1]);
  }

  @Override
  public int getSizeInBytes() {
    return size * 4 + 4;
  }
}
