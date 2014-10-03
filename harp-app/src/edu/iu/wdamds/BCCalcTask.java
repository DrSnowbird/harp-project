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
import edu.iu.harp.comm.data.FloatArray;
import edu.iu.harp.comm.data.ShortArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class BCCalcTask extends Task<RowData, XRowData> {

  /** Class logger */
  protected static final Logger LOG = Logger.getLogger(BCCalcTask.class);

  private final ArrPartition<DoubleArray>[] xPartitions;
  private final int d;
  private final double tCur;
  private final ResourcePool resourcePool;

  public BCCalcTask(ArrPartition<DoubleArray>[] xPartitions,
    int d, double tCur, ResourcePool resourcePool) {
    this.xPartitions = xPartitions;
    this.d = d;
    this.tCur = tCur;
    this.resourcePool = resourcePool;
  }

  @Override
  public XRowData run(RowData rowData) throws Exception {
    // Copy to local
    int d = this.d;
    double tCur = this.tCur;
    
    int bcSize = rowData.height * d;
    double[] bcs = this.resourcePool.getDoubleArrayPool().getArray(bcSize);
    // Matrix multiplication result array should be initialized as 0
    // This array is small. Initialization should be efficient.
    for (int i = 0; i < bcSize; i++) {
      if (bcs[i] != 0) {
        bcs[i] = 0;
      }
    }
    DoubleArray bcArray = new DoubleArray();
    bcArray.setArray(bcs);
    bcArray.setSize(bcSize);
    XRowData bcOutData = new XRowData();
    bcOutData.height = rowData.height;
    bcOutData.width = this.d;
    bcOutData.row = rowData.row;
    bcOutData.rowOffset = rowData.rowOffset;
    bcOutData.array = bcArray;
    calculateBC(rowData.distArray, rowData.weightArray, xPartitions,
      d, bcArray, rowData.row, rowData.rowOffset, rowData.height,
      rowData.width, tCur, resourcePool);
    return bcOutData;
  }

  void calculateBC(ShortArray distArray, DoubleArray weightArray,
    ArrPartition<DoubleArray>[] xPartitions, int d, DoubleArray bcArray,
    int row, int rowOffset, int height, int width, double tCur,
    ResourcePool resourcePool) throws Exception {
    int rowEnd = rowOffset + height;
    int bOfZSize = width * height;
    // long time0 = System.currentTimeMillis();
    double[] bOfZArr = resourcePool.getDoubleArrayPool().getArray(bOfZSize);
    DoubleArray bOfZ = new DoubleArray();
    bOfZ.setArray(bOfZArr);
    bOfZ.setSize(bOfZSize);
    // long time1 = System.currentTimeMillis();
    calculateBofZ(xPartitions, d, distArray, weightArray, bOfZ, row, rowOffset,
      rowEnd, height, width, tCur);
    // long time2 = System.currentTimeMillis();
    // Next we can calculate the BofZ * preX.
    // CalcUtil.matrixMultiply(bOfZ, xPartitions, xRowIndex, bcArray, height, d,
    //  width);
    CalcUtil.matrixMultiply(bOfZ, xPartitions, bcArray, height, d, width);
    // long time3 = System.currentTimeMillis();
    // System.out.println(" " + (time3 - time2) + " " + (time2 - time1) + " "
    //  + (time1 - time0));
    resourcePool.getDoubleArrayPool().releaseArrayInUse(bOfZArr);
  }

  private void calculateBofZ(ArrPartition<DoubleArray>[] xPartitions, int d,
    ShortArray distArray, DoubleArray weightArray, DoubleArray bofZ, int row,
    int rowOffset, int rowEnd, int height, int width, double tCur) {
    short[] dists = distArray.getArray();
    double[] weights = weightArray.getArray();
    double[] bOfZs = bofZ.getArray();
    double vBlockVal = -1;
    // Because tMax = maxOrigDistance / Math.sqrt(2.0 * d);
    // and tCur = alpha * tMax
    // diff must < maxOrigDistance
    double diff = Math.sqrt(2.0 * d) * tCur;
    if (tCur <= 0.000000001) {
      diff = 0;
    }
    // LOG.info("bc diff: " + diff);
    // To store the summation of each row
    double[] bOfZsIIArr = resourcePool.getDoubleArrayPool().getArray(height);
    int tmpI = 0;
    int tmpII = 0;
    int tmpIJ = 0;
    double distance = 0;
   //  int[] iID = new int[2];
    // iID[0] = row;
    // iID[1] = 0;
    // int[] jID = new int[2];

    
    /*
    for (int i = rowOffset; i < rowEnd; i++) {
      tmpI = i - rowOffset;
      tmpII = tmpI * width + i;
      jID[0] = 0;
      jID[1] = 0;
      for (int j = 0; j < width; j++) {
        if (i != j) {
          tmpIJ = tmpI * width + j;
          if (weights[tmpIJ] != 0) {
            // All i are from the same row partition
            iID[1] = tmpI;
            bOfZs[tmpIJ] = 0;
            distance = CalcUtil.calculateDistance(xPartitions, d, iID, jID);
            if (distance >= 1.0E-10 && diff < dists[tmpIJ]) {
               // This section follows the original code in case tCur >
               // 0.000000001. If tCur <= 0.000000001, we set diff = 0. We find
               // this code can still work to match the original code when tCur
               // <= 0.000000001 Notice that dist[tmpIJ] >= 0, if dist[tmpIJ] >
               // 0, we can enter here because diff = 0 < dist[tmpIJ]. Then the
               // formula is weights[tmpIJ] * vBlockVal * dists[tmpIJ] / distance
               // which is the same as original formula if dist[tmpIJ] == 0, then
               // we can not enter here but enter else clause. But if dist[tmpIJ]
               // == 0, then in if clause, bOfZs[tmpIJ] is also = 0 as what we
               // get from else clause
               
              bOfZs[tmpIJ] = weights[tmpIJ] * vBlockVal * (dists[tmpIJ] - diff)
                / distance;
            }
            // bOfZs[tmpII] is initialized as 0
            // Every time a value from i!=j is subtracted
            bOfZs[tmpII] = bOfZs[tmpII] - bOfZs[tmpIJ];
          }
        }
        CalcUtil.calculateNextXRowID(jID, xRowIndex);
      }
      
    }
    */
    
    double[] xiArr = xPartitions[row].getArray().getArray();
    int xiArrSize = xPartitions[row].getArray().getSize();
    double[] xjArr = null;
    int xjArrSize = 0;
    double tmpWeight = 0;
    double tmpDist = 0;
    double bOfZsII = 0;
    double bOfZsIJ = 0;
    tmpII = rowOffset;
    for (int i = 0; i < xiArrSize; i += d) {
      for (int j = 0; j < xPartitions.length; j++) {
        xjArr = xPartitions[j].getArray().getArray();
        xjArrSize = xPartitions[j].getArray().getSize();
        for (int k = 0; k < xjArrSize; k += d) {
          // II will be updated later
          if (tmpIJ != tmpII) {
            tmpWeight = weights[tmpIJ];
            if (tmpWeight != 0) {
              tmpDist = (double) dists[tmpIJ] / (double) Short.MAX_VALUE;
              bOfZsIJ = 0;
              distance = CalcUtil.calculateDistance(xiArr, i, xjArr, k, d);
              if (distance >= 1.0E-10 && diff < tmpDist) {
                bOfZsIJ = tmpWeight * vBlockVal * (tmpDist - diff) / distance;
              }
              bOfZsII = bOfZsII - bOfZsIJ;
              bOfZs[tmpIJ] = bOfZsIJ;
            } else {
              bOfZs[tmpIJ] = 0;
            }
          }
          tmpIJ++;
        }
      }
      bOfZsIIArr[tmpI] = bOfZsII;
      bOfZsII = 0;
      tmpI++;
      tmpII += width + 1;
    }
    // Update II position
    tmpII = rowOffset;
    for (int i = 0; i < height; i++) {
      bOfZs[tmpII] = bOfZsIIArr[i];
      tmpII += width + 1;
    }
    resourcePool.getDoubleArrayPool().releaseArrayInUse(bOfZsIIArr);
  }
}
