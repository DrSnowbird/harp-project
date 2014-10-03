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

package edu.iu.harp.comm.resource;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.Collections;

import org.apache.log4j.Logger;

public abstract class ArrayPool<T> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ArrayPool.class);
  // A map between size and buffer array (2 hashset. 0: free 1: in-use)
  private Int2ObjectOpenHashMap<ObjectOpenHashSet<T>[]> arrayMap;
  // A sorted list of free arrays
  private IntArrayList freeArrays;

  public ArrayPool() {
    arrayMap = new Int2ObjectOpenHashMap<ObjectOpenHashSet<T>[]>();
    freeArrays = new IntArrayList();
  }

  protected int getAdjustedArraySize(int size) {
    return (int) Math.pow(2, Math.ceil(Math.log(size) / Math.log(2)));
  }

  abstract protected T createNewArray(int size);

  protected abstract int getSizeOfArray(T array);

  public synchronized T getArray(int size) {
    int originSize = size;
    if (originSize <= 0) {
      return null;
    }
    // LOG.info("Original size: " + originSize);
    int adjustSize = getAdjustedArraySize(originSize);
    // LOG.info("Adjusted size: " + adjustSize);
    int pos = Collections.binarySearch(freeArrays, adjustSize);
    // LOG.info("Pos in free array: " + pos);
    /*
    if (pos < 0) {
      int insertPos = -1 * (pos + 1);
      // If this is an array with larger size
      // (not bigger than 2-fold) available
  
      if (insertPos < freeArrays.size()
        && freeArrays.get(insertPos) <= adjustSize * 2) {
        pos = insertPos;
        adjustSize = freeArrays.getInt(pos);
        // LOG.info("Array size at insert pos: " + freeArrays.getInt(pos));
      }
    }
    */

    T array = null;
    ObjectOpenHashSet<T>[] arrays = null;
    if (pos < 0) {
      // long start = System.currentTimeMillis();
      try {
        array = createNewArray(adjustSize);
      } catch (OutOfMemoryError e) {
        LOG.error("Out of memory in creating array with size " + adjustSize
          + " " + Runtime.getRuntime().totalMemory()
          + " " + Runtime.getRuntime().freeMemory(), e);
        return null;
      }
      // LOG.info("Create a new array with original size: " + originSize
      //  + ", adjusted size: " + adjustSize + ", with type: "
      //  + array.getClass().getName() + ", with time: "
      //  + (System.currentTimeMillis() - start));
      arrays = arrayMap.get(adjustSize);
      if (arrays == null) {
        // 0, buffer available, 1, buffer in use
        arrays = new ObjectOpenHashSet[2];
        arrays[0] = new ObjectOpenHashSet<T>();
        arrays[1] = new ObjectOpenHashSet<T>();
        arrayMap.put(adjustSize, arrays);
      }
      arrays[1].add(array);
    } else {
      // Remove it from buffers available
      freeArrays.remove(pos);
      // Since it is recorded in free buffers,
      // it must be in the map.
      arrays = arrayMap.get(adjustSize);
      // Get an array
      if(arrays == null) {
        // LOG.info("No such an array when getting an array with size: "
        //  + adjustSize);
      }
      ObjectIterator<T> iterator = arrays[0].iterator();
      array = iterator.next();
      // Move it from available to in-use
      arrays[0].remove(array);
      arrays[1].add(array);
      // LOG.info("Get an existing array with adjusted size: " + adjustSize
      //  + ", with type " + array.getClass().getName());
    }
    return array;
  }

  public synchronized boolean releaseArrayInUse(T array) {
    if (array == null) {
      LOG.info("Null array.");
      return false;
    }
    int size = getSizeOfArray(array);
    // LOG.info("Release an array with size: " + size + ", with type "
    //  + array.getClass().getName());
    ObjectOpenHashSet<T>[] arrays = arrayMap.get(size);
    if (arrays == null) {
      LOG.info("Fail to release an array with size: " + size + ", with type "
        + array.getClass().getName() + " no set");
      return false;
    }
    boolean result = arrays[1].remove(array);
    if (!result) {
      LOG.info("Fail to release an array with size: " + size + ", with type "
        + array.getClass().getName() + " no array");
      return false;
    }
    arrays[0].add(array);
    int pos = Collections.binarySearch(freeArrays, size);
    int insertPos = 0;
    if (pos >= 0) {
      // We may find it, but it may be for another array
      insertPos = pos;
    } else {
      insertPos = -1 * (pos + 1);
    }
    freeArrays.add(insertPos, size);
    return true;
  }

  public synchronized boolean freeArrayInUse(T array) {
    int size = getSizeOfArray(array);
    // LOG.info("Free an array with size: " + size + ", with type "
    //  + array.getClass().getName());
    ObjectOpenHashSet<T>[] arrays = arrayMap.get(size);
    if (arrays == null) {
      return false;
    }
    return arrays[1].remove(array);
  }

  public synchronized void freeAllArrays() {
    arrayMap.clear();
    freeArrays.clear();
  }
}
