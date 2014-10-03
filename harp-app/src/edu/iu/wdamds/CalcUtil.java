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
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.data.FloatArray;
import edu.iu.harp.comm.data.IntArray;

public class CalcUtil {

  /** Class logger */
  protected static final Logger LOG = Logger.getLogger(CalcUtil.class);

  /*
  public static boolean buildXRowIndex(ArrPartition<DoubleArray>[] x, int d,
    int[][] xRowIndex) {
    if (xRowIndex.length != 2 || x.length != xRowIndex[0].length) {
      return false;
    }
    int count = -1;
    for (int i = 0; i < x.length; i++) {
      // The first row is the row count of the partition
      // each element is the end row id of this partition
      xRowIndex[0][i] = x[i].getArray().getSize() / d - 1;
      // The second row is the cumulative row count
      count = count + x[i].getArray().getSize() / d;
      xRowIndex[1][i] = count;
    }
    return true;
  }
  */

  /**
   * Based on the last xID provided, calculate the one next to it. e.g. to
   * calculate i + 1 's position.
   * 
   * @param xRowID
   * @param xRowIndex
   */
  /*
  public static boolean calculateNextXRowID(int[] xRowID, int[][] xRowIndex) {
    // In each partition, xRowID starts at 0
    // end at row count - 1
    xRowID[1]++;
    if (xRowID[1] > xRowIndex[0][xRowID[0]]) {
      xRowID[0]++;
      xRowID[1] = 0;
    }
    if (xRowID[0] == xRowIndex[0].length) {
      return false;
    }
    return true;
  }
  */

  /**
   * Search through index to get correct i position in x array partitions.
   * 
   * @param i
   * @param xRowID
   * @param xRowIndex
   */
  /*
  public static boolean calculateXRowID(int i, int[] xRowID, int[][] xRowIndex) {
    xRowID[0] = Arrays.binarySearch(xRowIndex[1], i);
    if (xRowID[0] < 0) {
      xRowID[0] = -1 - xRowID[0]; // (xID[0] + 1) * -1
    }
    // Bigger than the biggest
    if (xRowID[0] == xRowIndex[1].length) {
      return false;
    }
    xRowID[1] = i;
    if (xRowID[0] > 0) {
      xRowID[1] = i - xRowIndex[1][xRowID[0]];
    }
    return true;
  }
  */

  public static double calculateDistance(ArrPartition<DoubleArray>[] x, int d,
    int[] iID, int[] jID) {
    ArrPartition<DoubleArray> xi = x[iID[0]];
    ArrPartition<DoubleArray> xj = x[jID[0]];
    double[] xiArr = xi.getArray().getArray();
    double[] xjArr = xj.getArray().getArray();
    double dist = 0;
    double diff = 0;
    int iStart = iID[1] * d;
    int jStart = jID[1] * d;
    for (int k = 0; k < d; k++) {
      diff = xiArr[iStart++] - xjArr[jStart++];
      dist += diff * diff;
    }
    dist = Math.sqrt(dist);
    return dist;
  }
  
  public static double calculateDistance(double[] xiArr, int iStart,
    double[] xjArr, int jStart, int d) {
    double dist = 0;
    double diff = 0;
    for (int k = 0; k < d; k++) {
      diff = xiArr[iStart++] - xjArr[jStart++];
      dist += diff * diff;
    }
    dist = Math.sqrt(dist);
    return dist;
  }

  /*
  public static void matrixMultiply(DoubleArray A,
    ArrPartition<DoubleArray>[] B, int[][] bParIndex, DoubleArray C,
    int aHeight, int bWidth, int len) throws Exception {
    // we need to cross the partitions for each column!
    double[] aArray = A.getArray();
    double[] cArray = C.getArray();
    if (C.getSize() != aHeight * bWidth) {
      throw new Exception("Cannot multiply matrices.");
    }
    double tmp = 0;
    int[] bParID = new int[2];
    double[] bParArr;
    for (int i = 0; i < aHeight; i++) {
      for (int j = 0; j < bWidth; j++) {
        tmp = 0;
        bParID[0] = 0;
        bParID[1] = 0;
        for (int k = 0; k < len; k++) {
          bParArr = B[bParID[0]].getArray().getArray();
          tmp = tmp + aArray[len * i + k] * bParArr[bWidth * bParID[1] + j];
          calculateNextXRowID(bParID, bParIndex);
        }
        cArray[i * bWidth + j] = tmp;
      }
    }
  }
  */

  /*
  public static void matrixMultiply(DoubleArray A,
    ArrPartition<DoubleArray>[] B, int[][] bParIndex, DoubleArray C,
    int aHeight, int bWidth, int len) throws Exception {
    if (C.getSize() != aHeight * bWidth) {
      throw new Exception("Cannot multiply matrices.");
    }
    double[] aArray = A.getArray();
    int aSize = A.getSize();
    int aRowStart = 0;
    double[] bParArr = null;
    int bParArrSize = 0;
    double[] cArray = C.getArray();
    int cStart = 0;
    double tmpA = 0;
    double tmpB = 0;
    double tmp = 0;
    for (int i = 0; i < aSize; i += len) {
      for (int j = 0; j < bWidth; j++) {
        aRowStart = i;
        tmp = 0;
        for (int k = 0; k < B.length; k++) {
          bParArr = B[k].getArray().getArray();
          bParArrSize = B[k].getArray().getSize();
          for (int l = 0; l < bParArrSize; l += bWidth) {
            tmpA = aArray[aRowStart++];
            tmpB = bParArr[l + j];
            if (tmpA != 0 && tmpB != 0) {
              tmp += tmpA * tmpB;
            }
          }
        }
        cArray[cStart++] = tmp;
      }
    }
  }
  */
  
  public static void matrixMultiply(DoubleArray A,
    ArrPartition<DoubleArray>[] B, DoubleArray C, int aHeight, int bWidth,
    int len) throws Exception {
    if (C.getSize() != aHeight * bWidth) {
      throw new Exception("Cannot multiply matrices.");
    }
    double[] aArray = A.getArray();
    int aSize = A.getSize();
    int aRowStart = 0;
    double[] bParArr = null;
    int bParArrSize = 0;
    int bRowStart = 0;
    double[] cArray = C.getArray();
    int cRowStart = 0;
    int tmpCRowStart = 0;
    // Go through rows in A
    for (int i = 0; i < aSize; i += len) {
      // Go through rows in B
      for (int j = 0; j < B.length; j++) {
        bParArr = B[j].getArray().getArray();
        bParArrSize = B[j].getArray().getSize();
        bRowStart = 0;
        for (int k = 0; k < bParArrSize; k += bWidth) {
          tmpCRowStart = cRowStart; // Get current row start in C
          for (int l = 0; l < bWidth; l++) {
            cArray[tmpCRowStart++] += aArray[aRowStart] * bParArr[bRowStart++];
          }
          aRowStart++; // Go to next element in A
        }
      }
      cRowStart += bWidth;
    }
  }
  
  public static void matrixMultiply(IntArray A,
    ArrPartition<DoubleArray>[] B, DoubleArray C, int aHeight, int bWidth,
    int len) throws Exception {
    if (C.getSize() != aHeight * bWidth) {
      throw new Exception("Cannot multiply matrices.");
    }
    int[] aArray = A.getArray();
    int aSize = A.getSize();
    int aRowStart = 0;
    double[] bParArr = null;
    int bParArrSize = 0;
    int bRowStart = 0;
    double[] cArray = C.getArray();
    int cRowStart = 0;
    int tmpCRowStart = 0;
    // Go through rows in A
    for (int i = 0; i < aSize; i += len) {
      // Go through rows in B
      for (int j = 0; j < B.length; j++) {
        bParArr = B[j].getArray().getArray();
        bParArrSize = B[j].getArray().getSize();
        bRowStart = 0; // This will go through each element
        for (int k = 0; k < bParArrSize; k += bWidth) {
          tmpCRowStart = cRowStart; // Get current row start in C
          for (int l = 0; l < bWidth; l++) {
            cArray[tmpCRowStart++] += aArray[aRowStart] * bParArr[bRowStart++];
          }
          aRowStart++; // Go to next element in A
        }
      }
      cRowStart += bWidth;
    }
  }
  
  public static void matrixMultiply(FloatArray A,
    ArrPartition<DoubleArray>[] B, DoubleArray C, int aHeight, int bWidth,
    int len) throws Exception {
    if (C.getSize() != aHeight * bWidth) {
      throw new Exception("Cannot multiply matrices.");
    }
    float[] aArray = A.getArray();
    int aSize = A.getSize();
    int aRowStart = 0;
    double[] bParArr = null;
    int bParArrSize = 0;
    int bRowStart = 0;
    double[] cArray = C.getArray();
    int cRowStart = 0;
    int tmpCRowStart = 0;
    // Go through rows in A
    for (int i = 0; i < aSize; i += len) {
      // Go through rows in B
      for (int j = 0; j < B.length; j++) {
        bParArr = B[j].getArray().getArray();
        bParArrSize = B[j].getArray().getSize();
        bRowStart = 0;
        for (int k = 0; k < bParArrSize; k += bWidth) {
          tmpCRowStart = cRowStart; // Get current row start in C
          for (int l = 0; l < bWidth; l++) {
            cArray[tmpCRowStart++] += aArray[aRowStart] * bParArr[bRowStart++];
          }
          aRowStart++; // Go to next element in A
        }
      }
      cRowStart += bWidth;
    }
  }
}
