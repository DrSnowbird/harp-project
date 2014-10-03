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

import java.util.Arrays;

import org.apache.log4j.Logger;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class StressCalcTask extends Task<RowData, DoubleArray> {

  /** Class logger */
  protected static final Logger LOG = Logger.getLogger(StressCalcTask.class);

  private final ArrPartition<DoubleArray>[] xPartitions;
  private final int d;
  private final double tCur;
  private final ResourcePool pool;

  public StressCalcTask(ArrPartition<DoubleArray>[] xPartitions, int d,
    double tCur, ResourcePool pool) {
    this.xPartitions = xPartitions;
    this.d = d;
    this.tCur = tCur;
    this.pool = pool;
  }

  @Override
  public DoubleArray run(RowData rowData) throws Exception {
    // Copy to local
    int d = this.d;
    double tCur = this.tCur;
    
    int rowEnd = rowData.rowOffset + rowData.height;
    short[] dists = rowData.distArray.getArray();
    double[] weights = rowData.weightArray.getArray();

    double diff = Math.sqrt(2.0 * d) * tCur;
    if (tCur <= 0) {
      diff = 0;
    }
    // LOG.info("stress diff: " + diff);
    int tmpI = 0;
    int tmpII = 0;
    int tmpIJ = 0;
    double distance;
    double dd;
    double sigma = 0;
    // int[] iID = new int[2];
    // iID[0] = rowData.row;
    // iID[1] = 0;
    // int[] jID = new int[2];
    /*
    for (int i = rowData.rowOffset; i < rowEnd; i++) {
      tmpI = i - rowData.rowOffset;
      jID[0] = 0;
      jID[1] = 0;
      for (int j = 0; j < rowData.width; j++) {
        tmpIJ = tmpI * rowData.width + j;
        if (weights[tmpIJ] != 0 && dists[tmpIJ] >= diff) {
          distance = 0;
          if (j != i) {
            // All i are from the same row partition
            iID[1] = tmpI;
            distance = CalcUtil.calculateDistance(xPartitions, d, iID, jID);
          }
          // dd = dists[tmpIJ] >= diff ? dists[tmpIJ] - diff - distance : 0;
          // If dists[tmpIJ] < diff, dd = 0, no need to add to sigma
          dd = dists[tmpIJ] - diff - distance;
          sigma += weights[tmpIJ] * dd * dd;
        }
        CalcUtil.calculateNextXRowID(jID, xRowIndex);
      }
    }
    */

    double[] xiArr = xPartitions[rowData.row].getArray().getArray();
    int xiArrSize = xPartitions[rowData.row].getArray().getSize();
    double[] xjArr = null;
    int xjArrSize = 0;
    double tmpWeight = 0;
    double tmpDist = 0;
    tmpII = rowData.rowOffset;
    for (int i = 0; i < xiArrSize; i += d) {
      for (int j = 0; j < xPartitions.length; j++) {
        xjArr = xPartitions[j].getArray().getArray();
        xjArrSize = xPartitions[j].getArray().getSize();
        for (int k = 0; k < xjArrSize; k += d) {
          tmpWeight = weights[tmpIJ];
          tmpDist = (double) dists[tmpIJ] / (double) Short.MAX_VALUE;
          if (tmpWeight != 0 && tmpDist >= diff) {
            distance = 0;
            if (tmpIJ != tmpII) {
              distance = CalcUtil.calculateDistance(xiArr, i, xjArr, k, d);
            }
            dd = tmpDist - diff - distance;
            sigma += tmpWeight * dd * dd;
          }
          tmpIJ++;
        }
      }
      tmpII += rowData.width + 1; // Get next II position
    }
    // Get double[1];
    double[] vals = pool.getDoubleArrayPool().getArray(1);
    vals[0] = sigma;
    DoubleArray output = new DoubleArray();
    output.setArray(vals);
    output.setSize(1);
    return output;
  }
}
