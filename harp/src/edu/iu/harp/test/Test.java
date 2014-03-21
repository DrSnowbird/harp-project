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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class Test {

  public static void main(String args[]) {
    /*
    Int2IntOpenHashMap map = new Int2IntOpenHashMap();
    int[] array = new int[map.size()];
    int[] arr = map.values().toArray(array);
    System.out.println(array.length + " " + arr.length);
    for(int i = 0; i< array.length; i++) {
      System.out.println(array[i]);
    }
    */
    int i = 0;
    int[] a = new int[10];
    for(int x = 2; x<= 10; x++) {
      a[i++] = x;
    }
    
    for(int j = 0; j <a.length; j++) {
      System.out.println(a[j]);
    }
  }
}
