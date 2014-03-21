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

import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class NoSyncCenCalcTask extends
  Task<DoubleArray, Map<Integer, DoubleArray>> {
  
  protected static final Log LOG = LogFactory.getLog(NoSyncCenCalcTask.class);
  private final ArrPartition<DoubleArray>[] cPartitions;

  private final int cParSize;
  private final int rest;
  private final int numCenPartitions;
  private final int vectorSize;
  private final ResourcePool resourcePool;
  private final NoSyncKMeansAllReduceMapper.Context context;
  
  private static final ThreadLocal<Map<Integer, DoubleArray>> cenDataMap = new ThreadLocal<Map<Integer, DoubleArray>>() {
    @Override
    protected Map<Integer, DoubleArray> initialValue() {
      return null;
    }
  };

  public NoSyncCenCalcTask(ArrPartition<DoubleArray>[] partitions,
    int cParSize, int rest, int numCenPartitions, int vectorSize,
    ResourcePool resourcePool, NoSyncKMeansAllReduceMapper.Context context) {
    this.cPartitions = partitions;
    this.cParSize = cParSize;
    this.rest = rest;
    this.numCenPartitions = numCenPartitions;
    this.vectorSize = vectorSize;
    this.resourcePool = resourcePool;
    this.context = context;
  }

  @Override
  public Map<Integer, DoubleArray> run(DoubleArray pData) throws Exception {
    if (cenDataMap.get() == null) {
      cenDataMap.set(NoSyncKMeansAllReduceMapper.createCenDataMap(cParSize,
        rest, numCenPartitions, this.vectorSize, this.resourcePool));
    }
    Map<Integer, DoubleArray> cenOutMap = cenDataMap.get();

    double[] pArray = pData.getArray();
    double distance = 0;
    double minDistance = 0;
    int[] minCenParIDPos = new int[2];
    minCenParIDPos[0] = 0;
    minCenParIDPos[1] = 0;
    DoubleArray cData = null;
    double[] cArray = null;
    for (int i = 0; i < pData.getSize(); i += vectorSize) {
      for (int j = 0; j < cPartitions.length; j++) {
        cData = cPartitions[j].getArray();
        cArray = cData.getArray();
        for (int k = 0; k < cData.getSize(); k += (vectorSize + 1)) {
          // Calculate distance for every two points
          distance = getEuclideanDist(pArray, cArray, i, k + 1, vectorSize);
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
      if (i % 1000 == 0) {
        context.progress();
      }
    }
    return cenOutMap;
  }

  private double getEuclideanDist(double[] pArray, double[] cArray, int pStart,
    int cStart, int vectorSize) {
    double sum = 0;
    double a = 0;
    for (int i = 0; i < vectorSize; i++) {
      // sum = sum + (pArray[pStart + i] - cArray[cStart + i])
      // * (pArray[pStart + i] - cArray[cStart + i]);
      a = pArray[pStart + i] - cArray[cStart + i];
      sum = sum + a * a;
    }
    return sum;
  }

  private void addPointToCenDataMap(Map<Integer, DoubleArray> cenOutMap,
    int[] minCenParIDPos, double[] pArray, int i, int vectorSize) {
    // Get output array partition
    double[] cOutArray = cenOutMap.get(minCenParIDPos[0]).getArray();
    // Count + 1
    cOutArray[minCenParIDPos[1]] = cOutArray[minCenParIDPos[1]] + 1;
    // Add the point
    for (int j = 0; j < vectorSize; j++) {
      cOutArray[minCenParIDPos[1] + 1 + j] = cOutArray[minCenParIDPos[1] + 1
        + j]
        + pArray[i + j];
    }
  }

  @SuppressWarnings("unused")
  private ArrPartition<DoubleArray>[] copyCenPartiitons(
    ArrPartition<DoubleArray>[] cPartitions) {
    ArrPartition<DoubleArray>[] cenPartitions = new ArrPartition[cPartitions.length];
    for (int i = 0; i < cPartitions.length; i++) {
      ArrPartition<DoubleArray> cPartition = cPartitions[i];
      DoubleArray doubleArray = new DoubleArray();
      doubleArray.setArray(new double[cPartition.getArray().getSize()]);
      doubleArray.setSize(cPartition.getArray().getSize());
      ArrPartition<DoubleArray> cenPartition = new ArrPartition<DoubleArray>(
        doubleArray, cPartition.getPartitionID());
      System.arraycopy(cPartition.getArray().getArray(), cPartition.getArray()
        .getStart(), doubleArray.getArray(), doubleArray.getStart(), cPartition
        .getArray().getSize());
      cenPartitions[i] = cenPartition;
    }
    return cenPartitions;
  }

  @SuppressWarnings("unused")
  private static Map<Integer, DoubleArray> createCenDataMap(int cenParSize,
    int rest, int numPartition, int vectorSize) {
    Map<Integer, DoubleArray> cOutMap = new Int2ObjectAVLTreeMap<DoubleArray>();
    for (int i = 0; i < numPartition; i++) {
      DoubleArray doubleArray = new DoubleArray();
      if (rest > 0) {
        // An extra element for every vector as count
        doubleArray.setArray(new double[(cenParSize + 1) * (vectorSize + 1)]);
        doubleArray.setSize((cenParSize + 1) * (vectorSize + 1));
        rest--;
      } else if (cenParSize > 0) {
        doubleArray.setArray(new double[cenParSize * (vectorSize + 1)]);
        doubleArray.setSize(cenParSize * (vectorSize + 1));
      } else {
        break;
      }
      cOutMap.put(i, doubleArray);
    }
    return cOutMap;
  }
}
