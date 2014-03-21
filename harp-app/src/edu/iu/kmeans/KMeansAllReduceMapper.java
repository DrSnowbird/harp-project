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

package edu.iu.kmeans;

import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CollectiveMapper;

import edu.iu.harp.arrpar.ArrCombiner;
import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.Double2DArrAvg;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class KMeansAllReduceMapper extends
  CollectiveMapper<String, String, Object, Object> {

  // private int jobID;
  // private int mapperID;
  private int numMappers;
  private int vectorSize;
  private int numCentroids;
  private int pointsPerFile;
  private int iteration;

  /**
   * Mapper configuration.
   */
  @Override
  protected void setup(Context context) throws IOException,
    InterruptedException {
    System.out.println("start setup"
      + new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance()
        .getTime()));
    long startTime = System.currentTimeMillis();

    Configuration configuration = context.getConfiguration();
    // jobID = Integer.parseInt(context.getJobName().split("_")[2]);
    // mapperID = this.getWorkerID();
    numMappers = configuration.getInt(KMeansConstants.NUM_MAPPERS, 10);
    numCentroids = configuration.getInt(KMeansConstants.NUM_CENTROIDS, 20);
    pointsPerFile = configuration.getInt(KMeansConstants.POINTS_PER_FILE, 20);
    vectorSize = configuration.getInt(KMeansConstants.VECTOR_SIZE, 20);
    iteration = configuration.getInt(KMeansConstants.ITERATION_COUNT, 1);
    long endTime = System.currentTimeMillis();
    LOG.info("config (ms) :" + (endTime - startTime));
  }

  protected void mapCollective(KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();
    LOG.info("Start collective mapper.");
    List<String> pointFiles = new ArrayList<String>();
    while (reader.nextKeyValue()) {
      String key = reader.getCurrentKey();
      String value = reader.getCurrentValue();
      LOG.info("Key: " + key + ", Value: " + value);
      pointFiles.add(value);
    }
    int numThreads = Runtime.getRuntime().availableProcessors() / 2;
    int numCenPartitions = this.numMappers;
    int vectorSize = this.vectorSize;
    Configuration conf = context.getConfiguration();
    runKmeans(pointFiles, numCenPartitions, vectorSize, numThreads, conf);
    LOG.info("Total iterations in master view: "
      + (System.currentTimeMillis() - startTime));
  }

  private void runKmeans(List<String> fileNames, int numCenPartitions,
    int vectorSize, int numThreads, Configuration conf) throws IOException {
    // ---------------------------------------------------------------------------
    // Load data points
    long startTime = System.currentTimeMillis();
    List<DoubleArray> pDataList = doTasks(fileNames, "load-data-points",
      new PointLoadTask(this.pointsPerFile, vectorSize, conf), numThreads);
    long endTime = System.currentTimeMillis();
    LOG.info("File read (ms): " + (endTime - startTime));
    // -----------------------------------------------------------------------------
    // Load centroids
    // Create centroid array partitions
    startTime = System.currentTimeMillis();
    int cParSize = this.numCentroids / numCenPartitions;
    int rest = this.numCentroids % numCenPartitions;
    // Note that per col size is vectorSize + 1;
    Map<Integer, DoubleArray> cenDataMap = createCenDataMap(cParSize, rest,
      numCenPartitions, vectorSize, this.getResourcePool());
    ArrTable<DoubleArray, DoubleArrPlus> table = new ArrTable<DoubleArray, DoubleArrPlus>(
      0, DoubleArray.class, DoubleArrPlus.class);
    if (this.isMaster()) {
      String cFile = conf.get(KMeansConstants.CFILE);
      loadCentroids(cenDataMap, vectorSize, cFile, conf);
      addPartitionMapToTable(cenDataMap, table);
    }
    endTime = System.currentTimeMillis();
    LOG.info("Load centroids (ms): " + (endTime - startTime));
    // Bcast centroids
    startTime = System.currentTimeMillis();
    bcastCentroids(table);
    endTime = System.currentTimeMillis();
    LOG.info("Bcast centroids (ms): " + (endTime - startTime));
    // -------------------------------------------------------------------------------------
    // For iterations
    ArrTable<DoubleArray, DoubleArrPlus> newTable = null;
    ArrPartition<DoubleArray>[] cPartitions = null;
    for (int i = 0; i < this.iteration; i++) {
      // For each iteration
      startTime = System.currentTimeMillis();
      cenDataMap = createCenDataMap(cParSize, rest, numCenPartitions,
        this.vectorSize, this.getResourcePool());
      cPartitions = table.getPartitions();
      doTasks(pDataList, "centroids-calc", new CenCalcTask(cPartitions,
        cenDataMap, vectorSize), numThreads);
      // Release centroids in this iteration
      releasePartitions(cPartitions);
      endTime = System.currentTimeMillis();
      LOG.info("Calculate centroids (ms): " + (endTime - startTime));
      cPartitions = null;
      // Allreduce
      table = new ArrTable<DoubleArray, DoubleArrPlus>(this.getWorkerID(),
        DoubleArray.class, DoubleArrPlus.class);
      addPartitionMapToTable(cenDataMap, table);
      cenDataMap = null;
      newTable = new ArrTable<DoubleArray, DoubleArrPlus>(1,
        DoubleArray.class, DoubleArrPlus.class);
      /*
       * Old table is altered during allreduce, ignore it. Allreduce:
       * regroup-combine-aggregate(reduce)-allgather table is at the state after
       * regroup-combine, but new table is after allgather. Since in
       * Double2DArrAvg, old table partition is moved to new table, we don't
       * need to release the partitions it uses.
       */
      try {
        startTime = System.currentTimeMillis();
        allreduce(table, newTable, new Double2DArrAvg(vectorSize + 1));
        endTime = System.currentTimeMillis();
        LOG.info("Allreduce time (ms): " + (endTime - startTime));
        table = null;
      } catch (Exception e) {
        LOG.error("Fail to do allreduce.", e);
        throw new IOException(e);
      }
      table = newTable;
    }
    // Write out new table
    if (this.isMaster()) {
      LOG.info("Start to write out centroids.");
      startTime = System.currentTimeMillis();
      storeCentroids(conf, this.getResourcePool(), newTable, vectorSize);
      endTime = System.currentTimeMillis();
      LOG.info("Store centroids time (ms): " + (endTime - startTime));
    }
    // Clean all the references
    newTable = null;
  }

  /**
   * Load data points from a file.
   * 
   * @param file
   * @param conf
   * @return
   * @throws IOException
   */
  public static DoubleArray loadPoints(String file, int pDataSize,
    Configuration conf) throws IOException {
    double[] pData = new double[pDataSize];
    Path inputFilePath = new Path(file);
    FileSystem fs = inputFilePath.getFileSystem(conf);
    // Read the input data file directly from HDFS
    System.out.println("Reading the file: " + inputFilePath.toString());
    FSDataInputStream in = fs.open(inputFilePath);
    try {
      for (int i = 0; i < pDataSize; i++) {
        pData[i] = in.readDouble();
      }
    } finally {
      in.close();
    }
    DoubleArray pArray = new DoubleArray();
    pArray.setArray(pData);
    pArray.setSize(pDataSize);
    return pArray;
  }

  /**
   * Fill data from centroid file to cenDataMap
   * 
   * @param cenDataMap
   * @param vectorSize
   * @param cFileName
   * @param configuration
   * @throws IOException
   */
  private static void loadCentroids(Map<Integer, DoubleArray> cenDataMap,
    int vectorSize, String cFileName, Configuration configuration)
    throws IOException {
    Path cPath = new Path(cFileName);
    FileSystem fs = FileSystem.get(configuration);
    FSDataInputStream in = fs.open(cPath);
    double[] cData;
    int start;
    int size;
    int collen = vectorSize + 1;
    for (DoubleArray array : cenDataMap.values()) {
      cData = array.getArray();
      start = array.getStart();
      size = array.getSize();
      for (int i = start; i < (start + size); i++) {
        // Don't set the first element in each row
        if (i % collen != 0) {
          cData[i] = in.readDouble();
        }
      }
    }
    in.close();
  }

  /**
   * Create array to store the intermediate centroid data in this iteration.
   * Each partition uses once dimensional array to present a two dimensional
   * array. The length of each row is vectorSize + 1, the first element is to
   * count how many data points are in this row during intermediate local sum.
   * 
   * @param cenParSize
   * @param rest
   * @param numPartition
   * @param vectorSize
   * @param resourcePool
   * @return
   */
  private static Map<Integer, DoubleArray> createCenDataMap(int cenParSize,
    int rest, int numPartition, int vectorSize, ResourcePool resourcePool) {
    Map<Integer, DoubleArray> cOutMap = new Int2ObjectAVLTreeMap<DoubleArray>();
    for (int i = 0; i < numPartition; i++) {
      DoubleArray doubleArray = new DoubleArray();
      if (rest > 0) {
        // An extra element for every vector as count
        doubleArray.setArray(resourcePool.getDoubleArrayPool().getArray(
          (cenParSize + 1) * (vectorSize + 1)));
        doubleArray.setSize((cenParSize + 1) * (vectorSize + 1));
        Arrays.fill(doubleArray.getArray(), 0);
        rest--;
      } else if (cenParSize > 0) {
        doubleArray.setArray(resourcePool.getDoubleArrayPool().getArray(
          cenParSize * (vectorSize + 1)));
        doubleArray.setSize(cenParSize * (vectorSize + 1));
        Arrays.fill(doubleArray.getArray(), 0);
      } else {
        break;
      }
      cOutMap.put(i, doubleArray);
    }
    return cOutMap;
  }
  
  /**
   * Broadcast centroids data in partitions
   * 
   * @param table
   * @param numPartitions
   * @throws IOException
   */
  private <T, A extends Array<T>, C extends ArrCombiner<A>> void bcastCentroids(
    ArrTable<A, C> table) throws IOException {
    boolean success = true;
    try {
      success = arrTableBcast(table);
    } catch (Exception e) {
      LOG.error("Fail to bcast.", e);
    }
    if (!success) {
      throw new IOException("Fail to bcast");
    }
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

  private void releasePartitions(ArrPartition<DoubleArray>[] partitions) {
    for (int i = 0; i < partitions.length; i++) {
      this.getResourcePool().getDoubleArrayPool()
        .releaseArrayInUse(partitions[i].getArray().getArray());
    }
  }

  private static void storeCentroids(Configuration configuration,
    ResourcePool resourcePool, ArrTable<DoubleArray, DoubleArrPlus> newTable,
    int vectorSize) throws IOException {
    String cFile = configuration.get(KMeansConstants.CFILE);
    Path cPath = new Path(cFile + "_out");
    LOG.info("centroids path: " + cPath.toString());
    FileSystem fs = FileSystem.get(configuration);
    fs.delete(cPath, true);
    FSDataOutputStream out = fs.create(cPath);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
    ArrPartition<DoubleArray> partitions[] = newTable.getPartitions();
    for (ArrPartition<DoubleArray> partition : partitions) {
      for (int i = 0; i < partition.getArray().getSize(); i++) {
        if (i % (vectorSize + 1) != 0) {
          // out.writeDouble(partition.getArray().getArray()[i]);
          bw.write(partition.getArray().getArray()[i] + " ");
        } else {
          bw.newLine();
        }
      }
      resourcePool.getDoubleArrayPool().freeArrayInUse(
        partition.getArray().getArray());
    }
    bw.flush();
    bw.close();
    // out.flush();
    // out.sync();
    // out.close();
  }
}
