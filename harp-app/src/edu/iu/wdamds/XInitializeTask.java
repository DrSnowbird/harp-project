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

import java.util.Random;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class XInitializeTask extends Task<RowData, ArrPartition<DoubleArray>> {

  private final int d;
  private final ResourcePool pool;

  public XInitializeTask(int d, ResourcePool pool) {
    this.d = d;
    this.pool = pool;
  }

  @Override
  public ArrPartition<DoubleArray> run(RowData rowData) throws Exception {
    // Copy to local
    int d = this.d;
    
    Random rand = new Random(rowData.rowOffset);
    DoubleArray doubleArray = new DoubleArray();
    int arrSize = rowData.height * d;
    double[] doubles = pool.getDoubleArrayPool().getArray(arrSize);
    doubleArray.setArray(doubles);
    doubleArray.setSize(arrSize);
    for (int i = 0; i < arrSize; i++) {
      if (rand.nextBoolean()) {
        doubles[i] = rand.nextDouble();
      } else {
        doubles[i] = 0 - rand.nextDouble();
      }
    }
    return new ArrPartition<DoubleArray>(doubleArray, rowData.row);
  }
}
