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

package edu.iu.harp.comm.data;

public class ByteArray extends Array<byte[]> {

  /**
   * The meta data is used to control the byte array communication in
   * complicated collective communication. It should be small with about 1~4
   * integers... not like array, meta array is always a seperated array. So its
   * always starts at 0.
   */
  private int[] metaArray;

  private int metaArraySize;

  public int[] getMetaArray() {
    return metaArray;
  }

  public int getMetaArraySize() {
    return metaArraySize;
  }

  public void setMetaArray(int[] metaArray) {
    this.metaArray = metaArray;
  }

  public void setMetaArraySize(int metaArraySize) {
    this.metaArraySize = metaArraySize;
  }
}
