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

package edu.iu.wdamds;

import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.data.ShortArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class AvgDistCalcTask extends Task<RowData, DoubleArray> {

  private final ResourcePool pool;

  public AvgDistCalcTask(ResourcePool pool) {
    this.pool = pool;
  }

  @Override
  public DoubleArray run(RowData rowData) throws Exception {
    ShortArray distArray = rowData.distArray;
    DoubleArray weightArray = rowData.weightArray;
    double average = 0;
    double avgSquare = 0;
    double maxDelta = 0;
    short[] distShorts = distArray.getArray();
    double[] weightDoubles = weightArray.getArray();
    // These two arrays should both start at 0
    // and have the same length.
    double distDouble = 0;
    int len = distArray.getSize();
    for (int i = 0; i < len; i++) {
      if (weightDoubles[i] != 0) {
        distDouble = (double) distShorts[i] / (double) Short.MAX_VALUE;
        average += distDouble;
        avgSquare += (distDouble * distDouble);
        if (maxDelta < distDouble) {
          maxDelta = distDouble;
        }
      }
    }
    DoubleArray output = new DoubleArray();
    // Get double[3];
    double[] vals = pool.getDoubleArrayPool().getArray(3);
    output.setArray(vals);
    output.setSize(3);
    vals[0] = average;
    vals[1] = avgSquare;
    vals[2] = maxDelta;
    return output;
  }
}
