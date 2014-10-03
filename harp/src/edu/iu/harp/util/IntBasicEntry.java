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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.Map;

/**
 * This class provides a basic but complete type-specific entry class for all
 * those maps implementations that do not have entries on their own (e.g., most
 * immutable maps).
 * 
 * <P>
 * This class does not implement {@link java.util.Map.Entry#setValue(Object)
 * setValue()}, as the modification would not be reflected in the base map.
 */
public class IntBasicEntry<V> implements Int2ObjectMap.Entry<V> {
  protected int key;
  protected V value;

  public IntBasicEntry(final Integer key, final V value) {
    this.key = ((key).intValue());
    this.value = (value);
  }

  public IntBasicEntry(final int key, final V value) {
    this.key = key;
    this.value = value;
  }

  public Integer getKey() {
    return (Integer.valueOf(key));
  }

  public int getIntKey() {
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
    return ((key) == (((((Integer) (e.getKey())).intValue()))))
      && ((value) == null ? ((e.getValue())) == null : (value).equals((e
        .getValue())));
  }

  public int hashCode() {
    return (key) ^ ((value) == null ? 0 : (value).hashCode());
  }

  public String toString() {
    return key + "->" + value;
  }
}
