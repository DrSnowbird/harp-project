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

package edu.iu.common;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.comm.data.DoubleArray;

public class MatrixUtils {

  public static void matrixMultiply(DoubleArray A, DoubleArray B,
    DoubleArray C, int aHeight, int bWidth, int len) {
    double[] aArray = A.getArray();
    double[] bArray = B.getArray();
    double[] cArray = C.getArray();
    if (C.getSize() != aHeight * bWidth) {
      return;
    }
    double tmp = 0;
    for (int i = 0; i < aHeight; i++) {
      for (int j = 0; j < bWidth; j++) {
        tmp = 0;
        for (int k = 0; k < len; k++) {
          tmp = tmp + aArray[len * i + k] * bArray[bWidth * k + j];
        }
        cArray[i * bWidth + j] = tmp;
      }
    }
  }

  public static void matrixMultiply(DoubleArray A,
    ArrPartition<DoubleArray>[] B, DoubleArray C, int aHeight, int bWidth,
    int len) {
    // we need to cross the partitions for each column!
    double[] aArray = A.getArray();
    double[] cArray = C.getArray();
    if (C.getSize() != aHeight * bWidth) {
      return;
    }
    double tmp = 0;
    double[] bParArr;
    double bParHeight = 0;
    for (int i = 0; i < aHeight; i++) {
      for (int j = 0; j < bWidth; j++) {
        tmp = 0;
        for (int k = 0; k < len; k++) {
          for (int l = 0; l < B.length; l++) {
            bParArr = B[l].getArray().getArray();
            bParHeight = B[l].getArray().getSize() / bWidth;
            for (int m = 0; m < bParHeight; m++) {
              tmp = tmp + aArray[len * i + k] * bParArr[bWidth * m + j];
            }
          }
        }
        cArray[i * bWidth + j] = tmp;
      }
    }
  }
}
