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

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.resource.DataDeserializer;
import edu.iu.harp.comm.resource.DataSerializer;
import edu.iu.harp.keyval.IntCountVal;
import edu.iu.harp.keyval.IntCountValPlus;
import edu.iu.harp.keyval.KeyValPartition;
import edu.iu.harp.keyval.StringKey;

public class KeyValPartitionTest {

  private static final Logger LOG = Logger.getLogger(KeyValPartitionTest.class);

  public static void main(String[] args) throws IOException,
    InstantiationException, IllegalAccessException, ClassNotFoundException {
    KeyValPartition<StringKey, IntCountVal, IntCountValPlus> partition = new KeyValPartition<StringKey, IntCountVal, IntCountValPlus>(
      0, 5, StringKey.class, IntCountVal.class, IntCountValPlus.class);
    partition.addKeyVal(new StringKey("a"), new IntCountVal(1, 1));
    partition.addKeyVal(new StringKey("b"), new IntCountVal(1, 1));
    partition.addKeyVal(new StringKey("c"), new IntCountVal(1, 1));
    partition.addKeyVal(new StringKey("d"), new IntCountVal(1, 1));
    partition.addKeyVal(new StringKey("e"), new IntCountVal(1, 1));
    partition.addKeyVal(new StringKey("a"), new IntCountVal(1, 1));

    // System.out.println(partition.getClass().getName());
    // System.out.println(partition.getCombiner().getClass().getName());

    byte[] bytes = new byte[partition.getSizeInBytes()];
    DataSerializer ds = new DataSerializer(bytes);
    partition.write(ds);

    KeyValPartition<StringKey, IntCountVal, IntCountValPlus> newPartition = (KeyValPartition<StringKey, IntCountVal, IntCountValPlus>) Class
      .forName(partition.getClass().getName()).newInstance();

    DataDeserializer dd = new DataDeserializer(bytes);
    newPartition.read(dd);
    System.out.println(newPartition.getClass().getName());
    System.out.println(newPartition.getKeyValMap().size());
    for (Entry<StringKey, IntCountVal> entry : newPartition.getKeyValMap()
      .entrySet()) {
      System.out.println(entry.getKey().getStringKey() + " "
        + entry.getValue().getIntValue());
    }
  }
}
