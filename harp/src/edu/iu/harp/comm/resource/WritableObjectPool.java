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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.data.WritableObject;

/**
 * Create and manage writable objects
 * 
 */
public class WritableObjectPool {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(WritableObjectPool.class);
  // A map between ref, and writable objects
  private Object2ObjectOpenHashMap<
    String, ObjectOpenHashSet<WritableObject>[]> writableObjectMap;

  public WritableObjectPool() {
    writableObjectMap = new Object2ObjectOpenHashMap<
      String, ObjectOpenHashSet<WritableObject>[]>();
  }

  public synchronized WritableObject getWritableObject(String className) {
    WritableObject obj = null;
    ObjectOpenHashSet<WritableObject>[] objectSets = writableObjectMap
      .get(className);
    if (objectSets != null && objectSets[0].size() > 0) {
      ObjectIterator<WritableObject> iterator = objectSets[0].iterator();
      obj = iterator.next();
      objectSets[0].remove(obj);
      objectSets[1].add(obj);
      // LOG.info("Get existing object " + className + ".");
    } else if (objectSets != null && objectSets[0].size() == 0) {
      obj = createWritableObject(className);
      if (obj != null) {
        objectSets[1].add(obj);
      }
    } else {
      obj = createWritableObject(className);
      if (obj != null) {
        objectSets = new ObjectOpenHashSet[2];
        objectSets[0] = new ObjectOpenHashSet<WritableObject>();
        objectSets[1] = new ObjectOpenHashSet<WritableObject>();
        writableObjectMap.put(className, objectSets);
        objectSets[1].add(obj);
      }
    }
    return obj;
  }

  private WritableObject createWritableObject(String className) {
    WritableObject obj = null;
    try {
      // For a random class name, it may not be a WritableObject
      obj = (WritableObject) Class.forName(className).newInstance();
      // LOG.info("Create a new object " + className + ".");
    } catch (Exception e) {
      // LOG.error("Fail to create object " + className + ".", e);
      obj = null;
    }
    return obj;
  }

  public synchronized boolean releaseWritableObjectInUse(WritableObject obj) {
    if (obj == null) {
      return true;
    }
    ObjectOpenHashSet<WritableObject>[] objectSets = writableObjectMap.get(obj
      .getClass().getName());
    if (objectSets == null) {
      return false;
    }
    boolean result = objectSets[1].remove(obj);
    if (!result) {
      return false;
    } else {
      objectSets[0].add(obj);
      // LOG.info("Release object " + obj.getClass().getName() + ".");
    }
    return true;
  }

  public synchronized boolean freeWritableObjectInUse(WritableObject obj) {
    ObjectOpenHashSet<WritableObject>[] objectSets =
      writableObjectMap.get(obj.getClass().getName());
    if(objectSets == null) {
      return false;
    }
    return objectSets[1].remove(obj);
  }

  public synchronized void freeAllWritableObjects() {
    writableObjectMap.clear();
  }
}
