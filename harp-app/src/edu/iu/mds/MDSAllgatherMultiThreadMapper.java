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

package edu.iu.mds;

import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CollectiveMapper;

import edu.iu.common.MatrixUtils;
import edu.iu.harp.arrpar.ArrCombiner;
import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class MDSAllgatherMultiThreadMapper extends
  CollectiveMapper<String, String, Object, Object> {

  private int iteration;
  private int numMappers;
  private int xWidth;
  private int numPoints;
  private int partitionPerWorker;
  private int totalPartitions;

  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    numPoints = conf.getInt(MDSConstants.NUMPOINTS, 10);
    xWidth = conf.getInt(MDSConstants.XWIDTH, 3);
    iteration = conf.getInt(MDSConstants.ITERATION, 1);
    numMappers = conf.getInt(MDSConstants.NUM_MAPS, 2);
    partitionPerWorker = conf.getInt(MDSConstants.PARTITION_PER_WORKER, 1);
    totalPartitions = partitionPerWorker * numMappers;
  }

  @Override
  public void mapCollective(KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    LOG.info("Start collective mapper.");
    List<String> dataFiles = new ArrayList<String>();
    while (reader.nextKeyValue()) {
      String key = reader.getCurrentKey();
      String value = reader.getCurrentValue();
      LOG.info("Key: " + key + ", Value: " + value);
      dataFiles.add(value);
    }
    int numThreads = 2;
    int numXPartitions = this.totalPartitions;
    Configuration conf = context.getConfiguration();
    runMDS(dataFiles, numXPartitions, numThreads, conf);
  }

  public void runMDS(List<String> dataFiles, int numXPartitions,
    int numThreads, Configuration conf) throws IOException,
    InterruptedException {
    // Load & Bcast X data
    ArrTable<DoubleArray, DoubleArrPlus> xDataTable = new ArrTable<DoubleArray, DoubleArrPlus>(
      0, DoubleArray.class, DoubleArrPlus.class);
    if (this.isMaster()) {
      int xParSize = this.numPoints / numXPartitions;
      int rest = this.numPoints % numXPartitions;
      Map<Integer, DoubleArray> xDataMap = createXDataMap(xParSize, rest,
        numXPartitions, this.xWidth, this.getResourcePool());
      String xFile = conf.get(MDSConstants.XFILE);
      loadXData(xDataMap, xFile, conf);
      addPartitionMapToTable(xDataMap, xDataTable);
    }
    bcastXData(xDataTable);
    // Load distance matrix
    List<Data> dataList = doTasks(dataFiles, "load-data-distances",
      new DataLoadTask(conf), numThreads);
    LOG.info("xWidth=" + xWidth + "\n" + " numPoints=" + numPoints + "\n");
    for (Data data : dataList) {
      LOG.info(" width=" + data.width + "\n" + " height=" + data.height + "\n"
        + " row=" + data.row + "\n" + " rowOffset=" + data.rowOffset + "\n");
    }
    // ---------------------------------------------------------------------------------
    // For iterations
    ArrPartition<DoubleArray>[] xPartitions = null;
    int[] xIndex = new int[this.totalPartitions];
    ArrPartition<DoubleArray> newPartition = null;
    ArrTable<DoubleArray, DoubleArrPlus> newXDataTable = null;
    for (int i = 0; i < iteration; i++) {
      xPartitions = xDataTable.getPartitions();
      buildXIndex(xPartitions, xIndex);
      List<Data> bcDataList = doTasks(
        dataList,
        "calculate-BC",
        new CalculateBCTask(xPartitions, xIndex, this.xWidth, this
          .getResourcePool()), numThreads);
      // Release partitions
      releasePartitions(xPartitions);
      xDataTable= null;
      newXDataTable = new ArrTable<DoubleArray, DoubleArrPlus>(0,
        DoubleArray.class, DoubleArrPlus.class);
      for (Data data : bcDataList) {
        newPartition = new ArrPartition<DoubleArray>(data.array, data.row);
        try {
          newXDataTable.addPartition(newPartition);
        } catch (Exception e) {
          LOG.error(e);
        }
      }
      allgather(newXDataTable);
      xDataTable = newXDataTable;
    }
    if (this.isMaster()) {
      storeX(conf, newXDataTable);
    }
  }

  private static Map<Integer, DoubleArray> createXDataMap(int xParSize,
    int rest, int numPartition, int xWidth, ResourcePool resourcePool) {
    Map<Integer, DoubleArray> xDataMap = new Int2ObjectAVLTreeMap<DoubleArray>();
    for (int i = 0; i < numPartition; i++) {
      DoubleArray doubleArray = new DoubleArray();
      if (rest > 0) {
        // An extra element for every vector as count
        doubleArray.setArray(resourcePool.getDoubleArrayPool().getArray(
          (xParSize + 1) * xWidth));
        doubleArray.setSize((xParSize + 1) * xWidth);
        Arrays.fill(doubleArray.getArray(), 0);
        rest--;
      } else if (xParSize > 0) {
        doubleArray.setArray(resourcePool.getDoubleArrayPool().getArray(
          xParSize * xWidth));
        doubleArray.setSize(xParSize * xWidth);
        Arrays.fill(doubleArray.getArray(), 0);
      } else {
        break;
      }
      xDataMap.put(i, doubleArray);
    }
    return xDataMap;
  }

  public void loadXData(Map<Integer, DoubleArray> xDataMap, String xFileName,
    Configuration configuration) throws IOException {
    Path xFilePath = new Path(xFileName);
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

  private <A extends Array<?>, C extends ArrCombiner<A>> void addPartitionMapToTable(
    Map<Integer, A> map, ArrTable<A, C> table) throws IOException {
    for (Entry<Integer, A> entry : map.entrySet()) {
      try {
        table
          .addPartition(new ArrPartition<A>(entry.getValue(), entry.getKey()));
      } catch (Exception e) {
        LOG.error("Fail to add partitions", e);
        throw new IOException(e);
      }
    }
  }

  private <T, A extends Array<T>, C extends ArrCombiner<A>> void bcastXData(
    ArrTable<A, C> table) throws IOException {
    boolean success = true;
    long startTime = System.nanoTime();
    try {
      success = arrTableBcast(table);
    } catch (Exception e) {
      LOG.error("Fail to bcast.", e);
    }
    long endTime = System.nanoTime();
    LOG.info("Bcast time: " + ((endTime - startTime) / 1000000));
    if (!success) {
      throw new IOException("Fail to bcast");
    }
  }

  public static Data loadData(String fileName, Configuration conf)
    throws IOException {
    DoubleArray array = null;
    Path inputFilePath = new Path(fileName);
    FileSystem fs = inputFilePath.getFileSystem(conf);
    // Begin reading the input data file
    System.out.println("Reading the file: " + inputFilePath.toString());
    Data data = null;
    FSDataInputStream in = fs.open(inputFilePath);
    try {
      int height = in.readInt();
      int width = in.readInt();
      int row = in.readInt();
      int rowOffset = in.readInt();
      double[] doubles = new double[height * width];
      for (int i = 0; i < doubles.length; i++) {
        doubles[i] = in.readDouble();
      }
      array = new DoubleArray();
      array.setArray(doubles);
      array.setSize(height * width);
      data = new Data();
      data.height = height;
      data.width = width;
      data.row = row;
      data.rowOffset = rowOffset;
      data.array = array;
    } finally {
      in.close();
    }
    // End reading the input data file
    return data;
  }

  static void calculateBC(DoubleArray data, ArrPartition<DoubleArray>[] xData,
    int[] xIndex, int xWidth, DoubleArray bcData, int rowOffset, int height,
    int width, ResourcePool resourcePool) {
    int end = rowOffset + height;
    int bOfZSize = width * height;
    double[] bOfZArr = resourcePool.getDoubleArrayPool().getArray(bOfZSize);
    DoubleArray bOfZ = new DoubleArray();
    bOfZ.setArray(bOfZArr);
    bOfZ.setSize(bOfZSize);
    calculateBofZ(xData, xIndex, xWidth, data, bOfZ, width, end, height,
      rowOffset);
    // Next we can calculate the BofZ * preX.
    MatrixUtils.matrixMultiply(bOfZ, xData, bcData, height, width, xWidth);
    resourcePool.getDoubleArrayPool().releaseArrayInUse(bOfZArr);
    double[] bcArr = bcData.getArray();
    for (int i = 0; i < bcData.getSize(); i++) {
      bcArr[i] = bcArr[i] / width;
    }
  }

  private static void calculateBofZ(ArrPartition<DoubleArray>[] x,
    int[] xIndex, int xWidth, DoubleArray data, DoubleArray bofZ, int width,
    int end, int height, int rowOffset) {
    double[] dataArr = data.getArray();
    double[] bOfZArr = bofZ.getArray();
    // This will store the summation
    int tmpI = 0;
    double vBlockVal = -1;
    double distance = 0;
    int[] iID = new int[2];
    int[] jID = new int[2];
    int indexII = 0;
    int indexIJ = 0;
    for (int i = rowOffset; i < end; i++) {
      tmpI = i - rowOffset;
      indexII = tmpI * width + i;
      for (int j = 0; j < width; j++) {
        if (i != j) {
          indexIJ = tmpI * width + j;
          getXID(i, iID, xIndex);
          getXID(j, jID, xIndex);
          LOG.info("i:" + i + " iID:" + iID[0] + "," + iID[1] + "\n" + "j:" + j
            + " jID:" + jID[0] + "," + jID[1] + "\n");
          distance = calculateDistance(x, xWidth, iID, jID);
          if (distance >= 1.0E-10) {
            bOfZArr[indexIJ] = vBlockVal * dataArr[indexIJ] / distance;
          } else {
            bOfZArr[indexIJ] = 0;
          }
          bOfZArr[indexII] = bOfZArr[indexII] - bOfZArr[indexIJ];
        }
      }
    }
  }

  private static double calculateDistance(ArrPartition<DoubleArray>[] x,
    int xWidth, int[] iID, int[] jID) {
    ArrPartition<DoubleArray> xi = x[iID[0]];
    ArrPartition<DoubleArray> xj = x[jID[0]];
    double[] xiArr = xi.getArray().getArray();
    double[] xjArr = xj.getArray().getArray();
    double dist = 0;
    double diff = 0;
    for (int k = 0; k < xWidth; k++) {
      diff = xiArr[iID[1] * xWidth + k] - xjArr[jID[1] * xWidth + k];
      dist += diff * diff;
    }
    dist = Math.sqrt(dist);
    return dist;
  }

  private static void getXID(int i, int[] xID, int[] xIndex) {
    int index = Arrays.binarySearch(xIndex, i);
    if (index < 0) {
      xID[0] = (index + 1) * -1 - 1;
    } else {
      xID[0] = index;
    }
    xID[1] = i - xID[0];
  }

  private void buildXIndex(ArrPartition<DoubleArray>[] x, int[] xIndex) {
    int count = 0;
    for (int i = 0; i < x.length; i++) {
      xIndex[i] = count;
      count = count + x[i].getArray().getSize() / this.xWidth;
    }
    if (x.length < xIndex.length) {
      Arrays.fill(xIndex, x.length, xIndex.length, count);
    }
  }

  private void releasePartitions(ArrPartition<DoubleArray>[] partitions) {
    for (int i = 0; i < partitions.length; i++) {
      this.getResourcePool().getDoubleArrayPool()
        .releaseArrayInUse(partitions[i].getArray().getArray());
    }
  }

  private void storeX(Configuration configuration,
    ArrTable<DoubleArray, DoubleArrPlus> table) throws IOException {
    Path xPath = new Path("x_out");
    LOG.info("x path: " + xPath.toString());
    FileSystem fs = FileSystem.get(configuration);
    fs.delete(xPath, true);
    FSDataOutputStream out = fs.create(xPath);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
    ArrPartition<DoubleArray> partitions[] = table.getPartitions();
    for (ArrPartition<DoubleArray> partition : partitions) {
      for (int i = 0; i < partition.getArray().getSize(); i++) {
        bw.write(partition.getArray().getArray()[i] + " ");
      }
      bw.newLine();
      this.getResourcePool().getDoubleArrayPool()
        .freeArrayInUse(partition.getArray().getArray());
    }
    bw.flush();
    bw.close();
  }
}
