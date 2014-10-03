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

package edu.iu.harp.util;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import java.util.Map;

/**
 * Modifed from FastUtil
 * 
 * @author zhangbj
 * 
 * @param <V> Entry value object
 */
public class LongBasicEntry<V> implements Long2ObjectMap.Entry<V> {
  protected long key;
  protected V value;

  public LongBasicEntry(final Long key, final V value) {
    this.key = ((key).longValue());
    this.value = (value);
  }

  public LongBasicEntry(final long key, final V value) {
    this.key = key;
    this.value = value;
  }

  public Long getKey() {
    return (Long.valueOf(key));
  }

  public long getLongKey() {
    return key;
  }

  public V getValue() {
    return (value);
  }

  public V setValue(final V value) {
    throw new UnsupportedOperationException();
  }

  public boolean equals(final Object o) {
    if (!(o instanceof Map.Entry))
      return false;
    Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
    return ((key) == (((((Long) (e.getKey())).longValue()))))
      && ((value) == null ? ((e.getValue())) == null : (value).equals((e
        .getValue())));
  }

  public int hashCode() {
    return it.unimi.dsi.fastutil.HashCommon.long2int(key)
      ^ ((value) == null ? 0 : (value).hashCode());
  }

  public String toString() {
    return key + "->" + value;
  }
}