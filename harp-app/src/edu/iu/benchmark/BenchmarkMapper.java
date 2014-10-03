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

package edu.iu.benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.CollectiveMapper;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class BenchmarkMapper extends
  CollectiveMapper<String, String, Object, Object> {

  private String cmd;
  private long totalBytes;
  private int numPartitions;
  private int numMappers;
  private int numIterations;
  private int tableID;

  /**
   * Mapper configuration.
   */
  @Override
  protected void setup(Context context) throws IOException,
    InterruptedException {
    Configuration configuration = context.getConfiguration();
    cmd = configuration.get(BenchmarkConstants.BENCHMARK_CMD, "bcast");
    numMappers = configuration.getInt(BenchmarkConstants.NUM_MAPPERS, 1);
    numPartitions = configuration.getInt(BenchmarkConstants.NUM_PARTITIONS, 1);
    totalBytes = configuration.getInt(BenchmarkConstants.TOTAL_BYTES, 1);
    numIterations = configuration.getInt(BenchmarkConstants.NUM_ITERATIONS, 1);
    tableID = 0;
  }

  protected void mapCollective(KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    // Read key-value pairs
    while (reader.nextKeyValue()) {
      String key = reader.getCurrentKey();
      String value = reader.getCurrentValue();
      LOG.info("Key: " + key + ", Value: " + value);
    }
    ResourcePool pool = getResourcePool();
    if (cmd.equals("allreduceone")) {
      long startTime = System.nanoTime();
      for (int i = 0; i < numIterations; i++) {
        // Create table
        ArrTable<DoubleArray, DoubleArrPlus> arrTable = new ArrTable<DoubleArray, DoubleArrPlus>(
          getNextTableID(), DoubleArray.class, DoubleArrPlus.class);
        // Create DoubleArray
        // Get double[1];
        double[] vals = pool.getDoubleArrayPool().getArray(1);
        vals[0] = 1;
        DoubleArray inArray = new DoubleArray();
        inArray.setArray(vals);
        inArray.setSize(1);
        // Create partition
        // Use uniformed partition id for combining
        ArrPartition<DoubleArray> arrPartition = new ArrPartition<DoubleArray>(
          inArray, 0);
        // Insert array to partition
        try {
          if (arrTable.addPartition(arrPartition)) {
            pool.getDoubleArrayPool().releaseArrayInUse(inArray.getArray());
          }
        } catch (Exception e) {
          LOG.error("Fail to add partition to table", e);
          break;
        }
        try {
          allgatherOne(arrTable);
        } catch (Exception e) {
          LOG.error("Fail to do allreduce one.", e);
          break;
        }
        DoubleArray outArray = arrTable.getPartitions()[0].getArray();
        // LOG.info("allreduce value: " + outArray.getArray()[0]);
        pool.getDoubleArrayPool().releaseArrayInUse(outArray.getArray());
      }
      long endTime = System.nanoTime();
      LOG.info("Total allreduce one time: " + (endTime - startTime)
        + " number of iterations: " + numIterations);
    } else if (cmd.equals("allgatherTotalKnown")) {
      int realNumPartitions = numPartitions / numMappers * numMappers;
      int numDoublesPerPartition = (int) (totalBytes / realNumPartitions / 8);
      int workerID = getWorkerID();
      List<ArrPartition<DoubleArray>> arrParList = new ArrayList<ArrPartition<DoubleArray>>();
      // Generate data
      for (int i = workerID; i < realNumPartitions; i += numMappers) {
        Random rand = new Random(i);
        DoubleArray doubleArray = new DoubleArray();
        int arrSize = numDoublesPerPartition;
        double[] doubles = pool.getDoubleArrayPool().getArray(arrSize);
        doubleArray.setArray(doubles);
        doubleArray.setSize(arrSize);
        for (int j = 0; j < arrSize; j++) {
          if (rand.nextBoolean()) {
            doubles[j] = rand.nextDouble();
          } else {
            doubles[j] = 0 - rand.nextDouble();
          }
        }
        arrParList.add(new ArrPartition<DoubleArray>(doubleArray, i));
      }
      LOG.info("Total partitions: " + realNumPartitions + " partition owned: "
        + arrParList.size());
      // Do iterations
      long totalTime = 0;
      LOG.info("numIterations: " + numIterations);
      for (int i = 0; i < numIterations; i++) {
        ArrTable<DoubleArray, DoubleArrPlus> arrTable = new ArrTable<DoubleArray, DoubleArrPlus>(
          getNextTableID(), DoubleArray.class, DoubleArrPlus.class);
        for (ArrPartition<DoubleArray> arrPar : arrParList) {
          try {
            arrTable.addPartition(arrPar);
          } catch (Exception e) {
            LOG.error("Fail to add partition to table", e);
            break;
          }
        }
        Thread.sleep(100);
        long startTime = System.nanoTime();
        try {
          allgatherTotalKnown(arrTable, realNumPartitions);
        } catch (Exception e) {
          LOG.error("Fail to do allgather total known", e);
          break;
        }
        long endTime = System.nanoTime();
        LOG.info("iteration " + i + " time: " + (endTime - startTime));
        totalTime += (endTime - startTime);
        ArrPartition<DoubleArray>[] partitions = arrTable.getPartitions();    
        // for (ArrPartition<DoubleArray> partition : partitions) {
        // LOG.info(partition.getPartitionID() + ":"
        // + Arrays.toString(partition.getArray().getArray()));
        // }
        // Release partitions got from other workers.
        for (int j = 0; j < partitions.length; j++) {
          if (partitions[j].getPartitionID() % numMappers != workerID) {
            pool.getDoubleArrayPool().releaseArrayInUse(
              partitions[j].getArray().getArray());
          }
        }
        // LOG.info("iteration " + i);
      }
      LOG.info("Total allgather total known time: " + totalTime
        + " number of iterations: " + numIterations);
    } else {
    }
  }

  private int getNextTableID() {
    return tableID++;
  }
}
