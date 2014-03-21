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

package edu.iu.optikmeans;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;

class NullCenDotProductTaskOut {
  NullCenDotProductTaskOut() {
  }
}

public class CenDotProductTask extends
  Task<ArrPartition<DoubleArray>, NullCenDotProductTaskOut> {

  final private int vectorSize;

  public CenDotProductTask(int vSize) {
    this.vectorSize = vSize;
  }

  @Override
  public NullCenDotProductTaskOut run(ArrPartition<DoubleArray> cenPartition)
    throws Exception {
    // Each centroid does dot product itself
    DoubleArray cenArray = cenPartition.getArray();
    double[] cenDoubles = cenArray.getArray();
    int cenStart = cenArray.getStart();
    int cenSize = cenArray.getSize();
    int vectorEnd;
    double sum = 0;
    for (int i = cenStart; i < cenSize; i = vectorEnd) {
      // Notice the length of every vector is vectorSize + 1
      vectorEnd = i + vectorSize + 1;
      sum = 0;
      for (int j = i + 1; j < vectorEnd; j++) {
        sum = sum + cenDoubles[j] * cenDoubles[j];
      }
      // An extra element at the beginning of each vector which is not used.
      cenDoubles[i] = sum;
    }
    return new NullCenDotProductTaskOut();
  }
}
