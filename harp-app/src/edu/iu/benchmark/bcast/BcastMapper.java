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

package edu.iu.benchmark.bcast;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.CollectiveMapper;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.data.DoubleArray;

public class BcastMapper extends
  CollectiveMapper<String, String, Object, Object> {

  // private int numMappers;
  private int numPartitions;
  private long totalBytes;
  private int numIterations;

  /**
   * Mapper configuration.
   */
  @Override
  protected void setup(Context context) throws IOException,
    InterruptedException {
    Configuration configuration = context.getConfiguration();
    // numMappers = configuration.getInt(BcastConstants.NUM_MAPPERS, 1);
    numPartitions = configuration.getInt(BcastConstants.NUM_PARTITIONS, 1);
    totalBytes = configuration.getInt(BcastConstants.TOTAL_BYTES, 1);
    numIterations = configuration.getInt(BcastConstants.NUM_ITERATIONS, 1);
  }

  protected void mapCollective(KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    while (reader.nextKeyValue()) {
      String key = reader.getCurrentKey();
      String value = reader.getCurrentValue();
      LOG.info("Key: " + key + ", Value: " + value);
    }
    ArrTable<DoubleArray, DoubleArrPlus> arrTable = new ArrTable<DoubleArray, DoubleArrPlus>(
      0, DoubleArray.class, DoubleArrPlus.class);
    // Generate data on master
    if (isMaster()) {
      long start = System.currentTimeMillis();
      for (int i = 0; i < numPartitions; i++) {
        DoubleArray doubleArray = generateDoubleArray(totalBytes
          / numPartitions);
        try {
          arrTable.addPartition(new ArrPartition<DoubleArray>(doubleArray, i));
        } catch (Exception e) {
          LOG.error("Fail to add partitions.", e);
        }
      }
      long end = System.currentTimeMillis();
      LOG.info("Double array data generation time: " + (end - start));
    }
    for (int i = 0; i < numIterations; i++) {
      if (isMaster()) {
        for (ArrPartition<DoubleArray> partition : arrTable.getPartitions()) {
          modifyNumbers(partition.getArray());
        }
      }
      long start = System.currentTimeMillis();
      arrTableBcast(arrTable);
      long end = System.currentTimeMillis();
      LOG.info("Total array table bcast time: " + (end - start));
      // If not master (any worker)
      if (!isMaster()) {
        // Release
        for (ArrPartition<DoubleArray> partition : arrTable.getPartitions()) {
          this.getResourcePool().getDoubleArrayPool()
            .releaseArrayInUse(partition.getArray().getArray());
        }
        // Create a new table
        arrTable = new ArrTable<DoubleArray, DoubleArrPlus>(0,
          DoubleArray.class, DoubleArrPlus.class);
      }
    }
    DoubleArray doubleArray = new DoubleArray();
    if (isMaster()) {
      doubleArray = generateDoubleArray(totalBytes);
    }
    for (int i = 0; i < numIterations; i++) {
      if (isMaster()) {
        modifyNumbers(doubleArray);
      }
      long start = System.currentTimeMillis();
      prmtvArrBcast(doubleArray);
      if (!isMaster()) {
        this.getResourcePool().getDoubleArrayPool()
          .releaseArrayInUse(doubleArray.getArray());
        doubleArray = new DoubleArray();
      }
      long end = System.currentTimeMillis();
      LOG.info("Total array bcast time: " + (end - start));
    }
  }

  private void modifyNumbers(DoubleArray doubleArray) {
    double[] doubles = doubleArray.getArray();
    doubles[doubleArray.getStart()] = Math.random() * 1000;
    doubles[doubleArray.getSize() - 1] = Math.random() * 1000;
  }

  private static DoubleArray generateDoubleArray(long totalByteData) {
    DoubleArray doubleArray = null;
    int size = (int) (totalByteData / (long) 8);
    double[] doubles = new double[size];
    doubles[0] = Math.random() * 1000;
    doubles[doubles.length - 1] = Math.random() * 1000;
    doubleArray = new DoubleArray();
    doubleArray.setArray(doubles);
    doubleArray.setStart(0);
    doubleArray.setSize(size);
    return doubleArray;
  }
}
