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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class CenCalcTask extends Task<DoubleArray, Map<Integer, DoubleArray>> {

  protected static final Log LOG = LogFactory.getLog(CenCalcTask.class);

  private final ArrPartition<DoubleArray>[] cenPartitions;
  private final int numCentroids;
  private final int numCenPartitions;
  private final int vectorSize;
  private final ResourcePool resourcePool;
  private final KMeansCollectiveMapper.Context context;
  private final AtomicLong appKernelTime;

  private static final ThreadLocal<Map<Integer, DoubleArray>> localCenParMap = new ThreadLocal<Map<Integer, DoubleArray>>() {
    @Override
    protected Map<Integer, DoubleArray> initialValue() {
      return null;
    }
  };

  public CenCalcTask(ArrPartition<DoubleArray>[] cenPartitions,
    int numCentroids, int numCenPartitions, int vectorSize,
    ResourcePool resourcePool, KMeansCollectiveMapper.Context context,
    AtomicLong appKernelTime) {
    this.cenPartitions = cenPartitions;
    this.numCentroids = numCentroids;
    this.numCenPartitions = numCenPartitions;
    this.vectorSize = vectorSize;
    this.resourcePool = resourcePool;
    this.context = context;
    this.appKernelTime = appKernelTime;
  }

  @Override
  public Map<Integer, DoubleArray> run(DoubleArray pointPartition)
    throws Exception {
    // Copy variables to thread local
    int vectorSize = this.vectorSize;
    int cenVecSize = vectorSize + 1;
    // Seems always exist in following iterations
    // So we just create without release
    if (localCenParMap.get() == null) {
      System.out.println("No thread local map.");
      // Using resource pool has better performance
      localCenParMap.set(KMeansCollectiveMapper.createCenParMap(numCentroids,
        numCenPartitions, cenVecSize, resourcePool));
    }
    Map<Integer, DoubleArray> cenParMap = localCenParMap.get();
    double[] points = pointPartition.getArray();
    double distance = 0;
    double minDistance = 0;
    // Change from double to float for app kernel benchmark
    // float distance = 0;
    // float minDistance = 0;
    int minCenParID = 0;
    int minCenVecStart = 0;
    double[] centroids = null;
    int cenSize = 0;
    // long appKernelStart = 0;
    // long appKernelEnd = 0;
    // long localAppKernelTime = 0;
    int count = 0;
    for (int i = 0; i < pointPartition.getSize(); i += vectorSize) {
      // appKernelStart = System.nanoTime();
      for (int j = 0; j < cenPartitions.length; j++) {
        // Computation intensive!
        // Try to make code simple and fast
        centroids = cenPartitions[j].getArray().getArray();
        cenSize = cenPartitions[j].getArray().getSize();
        for (int k = 0; k < cenSize; k += cenVecSize) {
          distance = getEuclideanDist(points, centroids, i, k, vectorSize);
          if (j == 0 && k == 0) {
            minDistance = distance;
          } else if (distance < minDistance) {
            minDistance = distance;
            minCenParID = j;
            minCenVecStart = k;
          }
        }
      }
      // appKernelEnd = System.nanoTime();
      // localAppKernelTime += (appKernelEnd - appKernelStart);
      addPointToCenParMap(cenParMap, minCenParID, minCenVecStart, points, i,
        vectorSize);
      // For every 100 points, report
      if (count % 1000 == 0) {
        context.progress();
      }
      count++;
    }
    // appKernelTime.addAndGet(localAppKernelTime);
    return cenParMap;
  }

  // Change from double to float for kernel benchmark
  // private float getEuclideanDist(double[] points, double[] centroids,
  // int pStart, int cStart, int vectorSize) {
  // cStart++;
  // float sum = 0;
  // float diff = 0;
  // for (int i = 0; i < vectorSize; i++) {
  // diff = (float) points[pStart++] - (float) centroids[cStart++];
  // sum += (diff * diff);
  // }
  // return sum;
  // }

  private double getEuclideanDist(double[] points, double[] centroids,
    int pStart, int cStart, int vectorSize) {
    // In distance calculation, dis = p^2 - 2pc + c^2
    // then dis / 2 = (p^2)/2 - pc + (c^2)/2
    // we don't need to calculate (p^2)/2, because it is same
    // (c^2) /2 is calculated in dot product
    double sum = centroids[cStart++];
    for (int i = 0; i < vectorSize; i++) {
      sum -= points[pStart++] * centroids[cStart++];
    }
    return sum;
  }

  private void addPointToCenParMap(Map<Integer, DoubleArray> localCenParMap,
    int minCenParID, int minCenVecStart, double[] points, int pStart,
    int vectorSize) {
    double[] centroids = localCenParMap.get(minCenParID).getArray();
    // Count + 1
    centroids[minCenVecStart++]++;
    // Add the point
    for (int j = 0; j < vectorSize; j++) {
      centroids[minCenVecStart++] += points[pStart++];
    }
  }
}
