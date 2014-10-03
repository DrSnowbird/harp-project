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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.comm.data.DoubleArray;

public class Test {

  public static void main(String[] args) {
    /*
    long startTime = System.currentTimeMillis();
    List<double[]> arrList = new ArrayList<double[]>(10000);
    for(int i = 0; i< 100000; i++) {
      arrList.add(new double[3]);
    }
    long endTime = System.currentTimeMillis();
    System.out.println("allocation time " + (endTime - startTime));
    */
    
    // 2*3
    /*
    double[] as = new double[6];
    Arrays.fill(as ,1);
    DoubleArray A = new DoubleArray();
    A.setArray(as);
    A.setSize(6);
    
    
    ArrPartition<DoubleArray>[] B = new ArrPartition[3];
    for(int i = 0; i< 3; i++) {
      double[] bs = new double[2];
      Arrays.fill(bs ,i);
      DoubleArray bParArr = new DoubleArray();
      bParArr.setArray(bs);
      bParArr.setSize(2);
      B[i] = new ArrPartition<DoubleArray>(bParArr, i);
    }
    
    // 2*2
    double[] cs = new double[4];
    Arrays.fill(cs ,0);
    DoubleArray C = new DoubleArray();
    C.setArray(cs);
    C.setSize(4);
    
    try {
      CalcUtil.matrixMultiply(A, B, C, 2, 2, 3);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    for(int i = 0; i < cs.length; i++) {
      System.out.println(cs[i]);
    }
    */
    
    int a = 2;
    double b = 3.8;
    double c = a*b;

  }

}
