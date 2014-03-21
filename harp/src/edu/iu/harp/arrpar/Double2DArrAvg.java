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

import edu.iu.harp.comm.data.DoubleArray;

public class Double2DArrAvg extends ArrConverter<DoubleArray, DoubleArray> {

  private int rowLen;

  public Double2DArrAvg(int rowLen) {
    this.rowLen = rowLen;
  }

  @Override
  public ArrPartition<DoubleArray> convert(ArrPartition<DoubleArray> op)
    throws Exception {
    // arr[0, 0 + rowLen, ...] row count
    double[] doubles = op.getArray().getArray();
    int size = op.getArray().getSize();
    if (size % rowLen != 0) {
      throw new Exception();
    }
    for (int i = 0; i < size; i = i + this.rowLen) {
      for (int j = 1; j < this.rowLen; j++) {
        // Calculate avg
        if(doubles[i] != 0) {
          doubles[i + j] = doubles[i + j] / doubles[i];
        }
      }
    }
    return op;
  }
}
