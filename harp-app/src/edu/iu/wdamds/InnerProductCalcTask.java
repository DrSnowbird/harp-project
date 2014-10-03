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

import java.util.Map;

import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class InnerProductCalcTask extends Task<XRowData, DoubleArray> {

  private final Map<Integer, XRowData> refMap;
  private final ResourcePool pool;

  public InnerProductCalcTask(Map<Integer, XRowData> refMap, ResourcePool pool) {
    this.refMap = refMap;
    this.pool = pool;
  }

  @Override
  public DoubleArray run(XRowData xRowData) throws Exception {
    double[] xrs = xRowData.array.getArray();
    int xRowSize = xRowData.array.getSize();
    double sum = 0;
    if (refMap == null) {
      for (int i = 0; i < xRowSize; i++) {
        sum += xrs[i] * xrs[i];
      }
    } else {
      // It is safe to get value from a shared map in parallel
      double[] rrs = refMap.get(xRowData.row).array.getArray();
      for (int i = 0; i < xRowSize; i++) {
        sum += xrs[i] * rrs[i];
      }
    }
    // Get double[1];
    double[] vals = pool.getDoubleArrayPool().getArray(1);
    vals[0] = sum;
    DoubleArray output = new DoubleArray();
    output.setArray(vals);
    output.setSize(1);
    return output;
  }
}
