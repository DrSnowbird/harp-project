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

package edu.iu.wdamds;

import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

class XFileUtil {

  static void loadXOnMaster(ArrTable<DoubleArray, DoubleArrPlus> xTable, int n,
    int d, int numMapTasks, boolean isMaster, ResourcePool resourcePool,
    String xFile, Configuration conf) throws IOException {
    if (isMaster) {
      // When loading from initial X file, we use the number of workers
      // as the number of total partitions
      int xParSize = n / numMapTasks;
      int rest = n % numMapTasks;
      Map<Integer, DoubleArray> xMap = createXMap(xParSize, rest, numMapTasks,
        d, resourcePool);
      loadX(xMap, xFile, conf);
      addPartitionMapToTable(xMap, xTable);
    }
  }

  private static void loadX(Map<Integer, DoubleArray> xDataMap, String xFile,
    Configuration configuration) throws IOException {
    Path xFilePath = new Path(xFile);
    FileSystem fs = FileSystem.get(configuration);
    FSDataInputStream in = fs.open(xFilePath);
    double[] xData;
    int start;
    int size;
    for (DoubleArray array : xDataMap.values()) {
      xData = array.getArray();
      start = array.getStart();
      size = array.getSize();
      for (int i = start; i < (start + size); i++) {
        xData[i] = in.readDouble();
      }
    }
    in.close();
  }

  private static Map<Integer, DoubleArray> createXMap(int xParSize, int rest,
    int numPartition, int xWidth, ResourcePool resourcePool) {
    Map<Integer, DoubleArray> xDataMap = new Int2ObjectAVLTreeMap<DoubleArray>();
    for (int i = 0; i < numPartition; i++) {
      DoubleArray doubleArray = new DoubleArray();
      if (rest > 0) {
        // An extra element for every vector as count
        doubleArray.setArray(resourcePool.getDoubleArrayPool().getArray(
          (xParSize + 1) * xWidth));
        doubleArray.setSize((xParSize + 1) * xWidth);
        // Arrays.fill(doubleArray.getArray(), 0);
        rest--;
      } else if (xParSize > 0) {
        doubleArray.setArray(resourcePool.getDoubleArrayPool().getArray(
          xParSize * xWidth));
        doubleArray.setSize(xParSize * xWidth);
        // Arrays.fill(doubleArray.getArray(), 0);
      } else {
        break;
      }
      xDataMap.put(i, doubleArray);
    }
    return xDataMap;
  }

  private static void addPartitionMapToTable(Map<Integer, DoubleArray> map,
    ArrTable<DoubleArray, DoubleArrPlus> table) throws IOException {
    for (Entry<Integer, DoubleArray> entry : map.entrySet()) {
      try {
        table.addPartition(new ArrPartition<DoubleArray>(entry.getValue(),
          entry.getKey()));
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  static void storeXOnMaster(Configuration configuration,
    ArrTable<DoubleArray, DoubleArrPlus> table, int d, String xOutFile,
    boolean isMaster) throws IOException {
    if (isMaster) {
      Path xOutPath = new Path(xOutFile);
      FileSystem fs = FileSystem.get(configuration);
      // fs.delete(xOutPath, true);
      FSDataOutputStream out = fs.create(xOutPath);
      PrintWriter writer = new PrintWriter(new BufferedWriter(
        new OutputStreamWriter(out)));
      DecimalFormat format = new DecimalFormat("#.##########");
      ArrPartition<DoubleArray> partitions[] = table.getPartitions();
      DoubleArray parArray;
      double[] doubles;
      int id = 0;
      for (ArrPartition<DoubleArray> partition : partitions) {
        parArray = partition.getArray();
        doubles = parArray.getArray();
        for (int i = 0; i < parArray.getSize(); i++) {
          if ((i % d) == 0) {
            writer.print(id + "\t");// print ID.
            id++;
          }
          writer.print(format.format(doubles[i]) + "\t");
          if ((i % d) == (d - 1)) {
            writer.println("1");
          }
        }
      }
      writer.flush();
      writer.close();
    }
  }

  static void storeXOnMaster(Configuration configuration,
    ArrTable<DoubleArray, DoubleArrPlus> table, int d, String xOutFile,
    boolean isMaster, String labelFile) throws IOException {
    if (isMaster) {
      // Read label file
      BufferedReader reader = new BufferedReader(new FileReader(labelFile));
      String line = null;
      String parts[] = null;
      Map<Integer, Integer> labels = new HashMap<Integer, Integer>();
      while ((line = reader.readLine()) != null) {
        parts = line.split(" ");
        if (parts.length < 2) {
          // Don't need to throw an error because this is the last part of
          // the computation
        }
        labels.put(Integer.parseInt(parts[1]), Integer.parseInt(parts[1]));
      }
      reader.close();
      Path xOutPath = new Path(xOutFile);
      FileSystem fs = FileSystem.get(configuration);
      // fs.delete(xOutPath, true);
      FSDataOutputStream out = fs.create(xOutPath);
      PrintWriter writer = new PrintWriter(new BufferedWriter(
        new OutputStreamWriter(out)));
      DecimalFormat format = new DecimalFormat("#.##########");
      ArrPartition<DoubleArray> partitions[] = table.getPartitions();
      DoubleArray parArray;
      double[] doubles;
      int id = 0;
      for (ArrPartition<DoubleArray> partition : partitions) {
        parArray = partition.getArray();
        doubles = parArray.getArray();
        for (int i = 0; i < parArray.getSize(); i++) {
          if ((i % d) == 0) {
            writer.print(id + "\t");// print ID.
            id++;
          }
          writer.print(format.format(doubles[i]) + "\t");
          writer.print(String.valueOf(i) + "\t");
          if ((i % d) == (d - 1)) {
            writer.println(labels.get(id));
          }
        }
      }
      writer.flush();
      writer.close();
    }
  }
}
