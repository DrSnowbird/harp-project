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

public class ResourcePool {

  private final ByteArrayPool byteArrays;

  private final IntArrayPool intArrays;
  
  private final FloatArrayPool floatArrays;

  private final DoubleArrayPool doubleArrays;

  private final WritableObjectPool objects;

  private final ByteArrayOutputStreamPool streams;

  public ResourcePool() {
    byteArrays = new ByteArrayPool();
    intArrays = new IntArrayPool();
    floatArrays = new FloatArrayPool();
    doubleArrays = new DoubleArrayPool();
    objects = new WritableObjectPool();
    streams = new ByteArrayOutputStreamPool();
  }

  public ByteArrayPool getByteArrayPool() {
    return byteArrays;
  }

  public IntArrayPool getIntArrayPool() {
    return intArrays;
  }

  public FloatArrayPool getFloatArrayPool() {
    return floatArrays;
  }

  public DoubleArrayPool getDoubleArrayPool() {
    return doubleArrays;
  }

  public WritableObjectPool getWritableObjectPool() {
    return objects;
  }

  public ByteArrayOutputStreamPool getByteArrayOutputStreamPool() {
    return streams;
  }
}
