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

package edu.iu.harp.test;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.lang.reflect.Array;

class GenericArray<T> {
  public T[] array;

  @SuppressWarnings("unchecked")
  public GenericArray(int n, Class<T> tClass) {
    array = (T[]) Array.newInstance(tClass, n);
    // array = (T[]) new Object[n];
  }

  public T[] getArray() {
    return array;
  }

  public void set(int i, T t) {
    array[i] = t;
  }

  public T get(int i) {
    return array[i];
  }
}

public class PrimitiveMapTest {

  public static void main(String args[]) {

    Int2ObjectOpenHashMap<double[]> map = new Int2ObjectOpenHashMap<double[]>();
    double[] doubles = new double[5];
    map.put(1, doubles);
    double[][] dbl2D = new double[5][5];
    if (doubles instanceof Object) {
      System.out.println("yes");
    }
    if (dbl2D[1] instanceof Object) {
      System.out.println("yes");
    }

    GenericArray<double[]> array = new GenericArray<double[]>(5, double[].class);

    double[][] dbls = array.getArray();
    if (dbls[1] == null) {
      System.out.println("null");
    }
    double[] dbl1 = { 1, 2, 3, 4 };
    dbls[1] = dbl1;

    System.out.println(dbls[1][1]);

    array.set(1, dbl1);
    if (array.get(1) != null) {
      System.out.println("yes, " + array.get(1).length);
    }

    double[][] prmvArray = new double[5][];
    double[] dbl2 = { 1, 2, 3, 4, 5 };
    prmvArray[0] = dbl1;
    prmvArray[1] = dbl2;

  }
}
