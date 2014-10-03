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

package edu.iu.kmeans;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;

class CenDotProductTaskOut {
  CenDotProductTaskOut() {
  }
}

public class CenDotProductTask extends
  Task<ArrPartition<DoubleArray>, CenDotProductTaskOut> {

  final private int cenVecSize;

  public CenDotProductTask(int vSize) {
    this.cenVecSize = vSize;
  }

  @Override
  public CenDotProductTaskOut run(ArrPartition<DoubleArray> cenPartition)
    throws Exception {
    // Copy to thread local
    int cenVecSize = this.cenVecSize;
    // Calculate (c^2)/2
    double[] centroids = cenPartition.getArray().getArray();
    int cenStart = cenPartition.getArray().getStart();
    int cenSize = cenPartition.getArray().getSize();
    int cenVecEnd = 0;
    double sum = 0;
    double a = 0;
    for (int i = cenStart; i < cenSize; i = cenVecEnd) {
      // Notice the length of every vector is vectorSize + 1
      cenVecEnd = i + cenVecSize;
      sum = 0;
      for (int j = i + 1; j < cenVecEnd; j++) {
        a = centroids[j];
        sum += a * a;
      }
      // An extra element at the beginning of each vector which is not used.
      centroids[i] = sum / 2;
    }
    return new CenDotProductTaskOut();
  }
}
