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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.CollectiveMapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.common.MatrixUtils;

public class MDSAllgatherMapper extends
  CollectiveMapper<String, String, Object, Object> {

  private int iteration;
  private int xWidth;
  private int numPoints;
  private int width;
  private int height;
  private int row;
  private int rowOffset;

  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    numPoints = conf.getInt(MDSConstants.NUMPOINTS, 10);
    xWidth = conf.getInt(MDSConstants.XWIDTH, 3);
    iteration = conf.getInt(MDSConstants.ITERATION, 1);
  }

  @Override
  public void mapCollective(KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    // Load data
    long startTime = System.nanoTime();
    Configuration conf = context.getConfiguration();
    DoubleArray xData = loadXData(conf);
    reader.nextKeyValue();
    String key = reader.getCurrentKey();
    String val = reader.getCurrentValue();
    DoubleArray data = loadInputData(val, conf);
    LOG.info("xWidth=" + xWidth + "\n" + " numPoints=" + numPoints + "\n"
      + " width=" + width + "\n" + " height=" + height + "\n" + " row=" + row
      + "\n" + " rowOffset=" + rowOffset + "\n");
    long endTime = System.nanoTime();
    LOG.info("config (ms) :" + ((endTime - startTime) / 1000000));
    // --------------------------------------------------------------------------------
    // Calculate BC
    startTime = System.nanoTime();
    DoubleArray cData = new DoubleArray();
    int cSize = height * xWidth;
    double[] cArray = this.getResourcePool().getDoubleArrayPool()
      .getArray(cSize);
    cData.setArray(cArray);
    cData.setSize(cSize);
    calculateBC(data, xData, cData, rowOffset, height, width);
    endTime = System.nanoTime();
    LOG.info("calculation(ms) :" + ((endTime - startTime) / 1000000));
    // Put cData to table, and do allgather
    ArrPartition<DoubleArray> cPartition = new ArrPartition<DoubleArray>(cData,
      row);
    ArrTable<DoubleArray, DoubleArrPlus> table = new ArrTable<DoubleArray, DoubleArrPlus>(
      0, DoubleArray.class, DoubleArrPlus.class);
    try {
      table.addPartition(cPartition);
    } catch (Exception e) {
      LOG.error(e);
    }
    allgather(table);
    // ---------------------------------------------------------------------------------
    // For rest iterations
    cData = null;
    xData = null;
    cArray = null;
    cPartition = null;
    ArrPartition<DoubleArray>[] xPartitions = table.getPartitions();
    int[] xIndex = new int[xPartitions.length];
    buildXIndex(xPartitions, xIndex);
    for (int i = 1; i < iteration; i++) {
      cArray = this.getResourcePool().getDoubleArrayPool().getArray(cSize);
      cData = new DoubleArray();
      cData.setArray(cArray);
      cData.setSize(cSize);
      calculateBC(data, xPartitions, xIndex, cData, rowOffset, height, width);
      // Release partitions
      releasePartitions(xPartitions);
      table = new ArrTable<DoubleArray, DoubleArrPlus>(0, DoubleArray.class,
        DoubleArrPlus.class);
      cPartition = new ArrPartition<DoubleArray>(cData, row);
      try {
        table.addPartition(cPartition);
      } catch (Exception e) {
        LOG.error(e);
      }
      allgather(table);
      xPartitions = table.getPartitions();
    }
    if (this.isMaster()) {
      storeX(context.getConfiguration(), table);
    }
  }

  public DoubleArray loadXData(Configuration configuration) throws IOException {
    double[] xData = new double[numPoints * xWidth];
    String xFile = configuration.get(MDSConstants.XFILE);
    Path xFilePath = new Path(xFile);
    FileSystem fs = FileSystem.get(configuration);
    FSDataInputStream in = fs.open(xFilePath);
    for (int i = 0; i < xData.length; i++) {
      xData[i] = in.readDouble();
    }
    in.close();
    DoubleArray xArray = new DoubleArray();
    xArray.setArray(xData);
    xArray.setSize(xData.length);
    return xArray;
  }

  public DoubleArray loadInputData(String value, Configuration conf)
    throws IOException {
    DoubleArray dataArray = null;
    Path inputFilePath = new Path(value);
    FileSystem fs = inputFilePath.getFileSystem(conf);
    // Begin reading the input data file
    System.out.println("Reading the file: " + inputFilePath.toString());
    FSDataInputStream in = fs.open(inputFilePath);
    try {
      height = in.readInt();
      width = in.readInt();
      row = in.readInt();
      rowOffset = in.readInt();
      double[] data = new double[height * width];
      for (int i = 0; i < data.length; i++) {
        data[i] = in.readDouble();
      }
      dataArray = new DoubleArray();
      dataArray.setArray(data);
      dataArray.setSize(height * width);
    } finally {
      in.close();
    }
    // End reading the input data file
    return dataArray;
  }

  private void calculateBC(DoubleArray data, DoubleArray xData,
    DoubleArray cData, int rowOffset, int height, int width) {
    int end = rowOffset + height;
    int bOfZSize = width * height;
    double[] bOfZArr = this.getResourcePool().getDoubleArrayPool()
      .getArray(bOfZSize);
    DoubleArray bOfZ = new DoubleArray();
    bOfZ.setArray(bOfZArr);
    bOfZ.setSize(bOfZSize);
    calculateBofZ(xData, data, bOfZ, width, end, height, rowOffset);
    // Next we can calculate the BofZ * preX.
    MatrixUtils.matrixMultiply(bOfZ, xData, cData, height, width, xWidth);
    this.getResourcePool().getDoubleArrayPool().releaseArrayInUse(bOfZArr);
    double[] cArr = cData.getArray();
    for (int i = 0; i < cData.getSize(); i++)
      cArr[i] = cArr[i] / width;
  }

  private void calculateBofZ(DoubleArray x, DoubleArray data, DoubleArray bOfZ,
    int width, int end, int height, int rowOffset) {
    double[] dataArr = data.getArray();
    double[] bOfZArr = bOfZ.getArray();
    double[] xArr = x.getArray();
    // This will store the summation
    int tmpI = 0;
    double vBlockVal = -1;
    double distance = 0;
    int indexII = 0;
    int indexIJ = 0;
    for (int i = rowOffset; i < end; i++) {
      tmpI = i - rowOffset;
      indexII = tmpI * width + i;
      for (int j = 0; j < width; j++) {
        if (i != j) {
          indexIJ = tmpI * width + j;
          distance = calculateDistance(xArr, xWidth, i, j);
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

  private static double calculateDistance(double[] x, int xWidth, int i, int j) {
    double dist = 0;
    double diff = 0;
    for (int k = 0; k < xWidth; k++) {
      diff = x[i * xWidth + k] - x[j * xWidth + k];
      dist += diff * diff;
    }
    dist = Math.sqrt(dist);
    return dist;
  }

  private void calculateBC(DoubleArray data, ArrPartition<DoubleArray>[] xData,
    int[] xIndex, DoubleArray cData, int rowOffset, int height, int width) {
    int end = rowOffset + height;
    int bOfZSize = width * height;
    double[] bOfZArr = this.getResourcePool().getDoubleArrayPool()
      .getArray(bOfZSize);
    DoubleArray bOfZ = new DoubleArray();
    bOfZ.setArray(bOfZArr);
    bOfZ.setSize(bOfZSize);
    calculateBofZ(xData, xIndex, data, bOfZ, width, end, height, rowOffset);
    // Next we can calculate the BofZ * preX.
    MatrixUtils.matrixMultiply(bOfZ, xData, cData, height, width, xWidth);
    this.getResourcePool().getDoubleArrayPool().releaseArrayInUse(bOfZArr);
    double[] cArr = cData.getArray();
    for (int i = 0; i < cData.getSize(); i++)
      cArr[i] = cArr[i] / width;
  }

  private void calculateBofZ(ArrPartition<DoubleArray>[] x, int[] xIndex,
    DoubleArray data, DoubleArray bofZ, int width, int end, int height,
    int rowOffset) {
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

  private void getXID(int i, int[] xID, int[] xIndex) {
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
    xIndex[0] = 0;
    for (int i = 1; i < x.length; i++) {
      count = count + x[i].getArray().getSize() / this.xWidth;
      xIndex[i] = count;
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
