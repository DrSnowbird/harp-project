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
import java.util.concurrent.ConcurrentHashMap;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;

class Null {
  public Null() {
  }
}

public class CenCalcTask extends Task<DoubleArray, Null> {
  private final ArrPartition<DoubleArray>[] cPartitions;
  private final Map<Integer, DoubleArray> cenOutMap;
  private final int vectorSize;

  public CenCalcTask(ArrPartition<DoubleArray>[] partitions,
    Map<Integer, DoubleArray> cenData, int size) {
    this.cPartitions = partitions;
    this.cenOutMap = new ConcurrentHashMap<Integer, DoubleArray>(cenData);
    this.vectorSize = size;
  }

  @Override
  public Null run(DoubleArray pData) throws Exception {
    double[] pArray = pData.getArray();
    double distance = 0;
    double minDistance = 0;
    int[] minCenParIDPos = new int[2];
    minCenParIDPos[0] = 0;
    minCenParIDPos[1] = 0;
    DoubleArray cData = null;
    double[] cArray = null;
    for (int i = 0; i < pData.getSize(); i += this.vectorSize) {
      for (int j = 0; j < cPartitions.length; j++) {
        cData = cPartitions[j].getArray();
        cArray = cData.getArray();
        for (int k = 0; k < cData.getSize(); k += (this.vectorSize + 1)) {
          // Calculate distance for every two points
          distance = getEuclideanDist(pArray, cArray, i, k + 1, this.vectorSize);
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
    }
    return new Null();
  }

  private double getEuclideanDist(double[] pArray, double[] cArray, int pStart,
    int cStart, int vectorSize) {
    double sum = 0;
    for (int i = 0; i < vectorSize; i++) {
      sum = sum + (pArray[pStart + i] - cArray[cStart + i])
        * (pArray[pStart + i] - cArray[cStart + i]);
    }
    return sum;
  }

  private void addPointToCenDataMap(Map<Integer, DoubleArray> cenOutMap,
    int[] minCenParIDPos, double[] pArray, int i, int vectorSize) {
    // Get output array partition
    double[] cOutArray = cenOutMap.get(minCenParIDPos[0]).getArray();
    synchronized (cOutArray) {
      // Count + 1
      cOutArray[minCenParIDPos[1]] = cOutArray[minCenParIDPos[1]] + 1;
      // Add the point
      for (int j = 0; j < vectorSize; j++) {
        cOutArray[minCenParIDPos[1] + 1 + j] = cOutArray[minCenParIDPos[1] + 1
          + j]
          + pArray[i + j];
      }
    }
  }
}
