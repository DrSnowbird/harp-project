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

import java.util.Map;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class CenCalcTask extends Task<DoubleArray, Map<Integer, DoubleArray>> {

  // protected static final Log LOG = LogFactory.getLog(CenCalcTask.class);
  private final ArrPartition<DoubleArray>[] cenPartitions;

  private final int cenParSize;
  private final int cenRest;
  private final int numCenPartitions;
  private final int vectorSize;
  private final ResourcePool resourcePool;
  private final KMeansAllReduceMapper.Context context;

  private static final ThreadLocal<Map<Integer, DoubleArray>> cenDataMap = new ThreadLocal<Map<Integer, DoubleArray>>() {
    @Override
    protected Map<Integer, DoubleArray> initialValue() {
      return null;
    }
  };

  public CenCalcTask(ArrPartition<DoubleArray>[] partitions, int cParSize,
    int rest, int numCenPartitions, int vectorSize, ResourcePool resourcePool,
    KMeansAllReduceMapper.Context context) {
    this.cenPartitions = partitions;
    this.cenParSize = cParSize;
    this.cenRest = rest;
    this.numCenPartitions = numCenPartitions;
    this.vectorSize = vectorSize;
    this.resourcePool = resourcePool;
    this.context = context;
  }

  @Override
  public Map<Integer, DoubleArray> run(DoubleArray pData) throws Exception {
    if (cenDataMap.get() == null) {
      cenDataMap.set(KMeansAllReduceMapper.createCenDataMap(cenParSize,
        cenRest, numCenPartitions, vectorSize, resourcePool));
    }
    Map<Integer, DoubleArray> cenOutMap = cenDataMap.get();
    double[] pArray = pData.getArray();
    DoubleArray cenArray = null;
    double[] cenDoubles = null;
    // Distance and position recording
    double distance = 0;
    double minDistance = 0;
    int[] minCenParIDPos = new int[2];
    minCenParIDPos[0] = 0;
    minCenParIDPos[1] = 0;
    int cenVectorLen = vectorSize + 1;
    for (int i = 0; i < pData.getSize(); i += vectorSize) {
      for (int j = 0; j < cenPartitions.length; j++) {
        cenArray = cenPartitions[j].getArray();
        cenDoubles = cenArray.getArray();
        for (int k = 0; k < cenArray.getSize(); k += cenVectorLen) {
          // Calculate distance for every two points
          distance = getEuclideanDist(pArray, cenDoubles, i, k, vectorSize);
          if (j == 0 && k == 0) {
            minDistance = distance;
            minCenParIDPos[0] = j;
            minCenParIDPos[1] = k;
          } else if (distance < minDistance) {
            minDistance = distance;
            minCenParIDPos[0] = j;
            minCenParIDPos[1] = k;
          }
        }
      }
      addPointToCenDataMap(cenOutMap, minCenParIDPos, pArray, i, vectorSize);
      // if (i % 1000 == 0) {
      // context.progress();
      // }
    }
    return cenOutMap;
  }

  private double getEuclideanDist(double[] pArray, double[] cArray, int pStart,
    int cStart, int vectorSize) {
    // In distance calculation, dis = p^2 - 2pc + c^2
    // we don't need to calculate p^2, because it is same
    // for a p in all distance calculation. 2p is calculated and stored
    // in pArray
    double sum = cArray[cStart++];
    for (int i = 0; i < vectorSize; i++) {
      sum -= pArray[pStart++] * cArray[cStart++];
    }
    return sum;
  }

  private void addPointToCenDataMap(Map<Integer, DoubleArray> cenOutMap,
    int[] minCenParIDPos, double[] pArray, int pStart, int vectorSize) {
    int cStart = minCenParIDPos[1];
    // Get output array partition
    double[] cOutArray = cenOutMap.get(minCenParIDPos[0]).getArray();
    // Count + 1
    cOutArray[cStart++]++;
    // Add the point
    for (int j = 0; j < vectorSize; j++) {
      cOutArray[cStart++] += pArray[pStart++];
    }
  }
}
