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

import java.util.Map;
import java.util.Set;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;

class CenMergeTaskOut {
  public CenMergeTaskOut() {
  }
}

public class CenMergeTask extends
  Task<ArrPartition<DoubleArray>, CenMergeTaskOut> {
  private final Set<Map<Integer, DoubleArray>> localCenParMaps;

  public CenMergeTask(Set<Map<Integer, DoubleArray>> localCenParMaps) {
    this.localCenParMaps = localCenParMaps;
  }

  @Override
  public CenMergeTaskOut run(ArrPartition<DoubleArray> cenPartition)
    throws Exception {
    int partitionID = cenPartition.getPartitionID();
    double[] centroids = cenPartition.getArray().getArray();
    DoubleArray localCenPartition = null;
    double[] localCentroids = null;
    int localCenParSize = 0;
    boolean first = true;
    // It is safe to iterate concurrently
    // because each task has its own iterator
    for (Map<Integer, DoubleArray> localCenParMap : localCenParMaps) {
      localCenPartition = localCenParMap.get(partitionID);
      localCentroids = localCenPartition.getArray();
      localCenParSize = localCenPartition.getSize();
      for (int i = 0; i < localCenParSize; i++) {
        if (first) {
          centroids[i] = 0;
        }
        centroids[i] += localCentroids[i];
        // Clean local centroids
        localCentroids[i] = 0;
      }
      if (first) {
        first = false;
      }
    }
    return new CenMergeTaskOut();
  }
}
