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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

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

public class KMeansCollectiveMapper extends
  CollectiveMapper<String, String, Object, Object> {

  private int jobID;
  private int numMappers;
  private int vectorSize;
  private int cenVecSize;
  private int numCentroids;
  private int numCenPartitions;
  private int pointsPerFile;
  private int iteration;
  private int numMaxThreads;

  /**
   * Mapper configuration.
   */
  @Override
  protected void setup(Context context) throws IOException,
    InterruptedException {
    LOG.info("start setup"
      + new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance()
        .getTime()));
    long startTime = System.currentTimeMillis();
    Configuration configuration = context.getConfiguration();
    jobID = configuration.getInt(KMeansConstants.JOB_ID, 0);
    numMappers = configuration.getInt(KMeansConstants.NUM_MAPPERS, 10);
    numCentroids = configuration.getInt(KMeansConstants.NUM_CENTROIDS, 20);
    numCenPartitions = numMappers;
    pointsPerFile = configuration.getInt(KMeansConstants.POINTS_PER_FILE, 20);
    vectorSize = configuration.getInt(KMeansConstants.VECTOR_SIZE, 20);
    cenVecSize = vectorSize + 1;
    iteration = configuration.getInt(KMeansConstants.ITERATION_COUNT, 1);
    numMaxThreads = 32;
    long endTime = System.currentTimeMillis();
    LOG.info("config (ms) :" + (endTime - startTime));
  }

  protected void mapCollective(KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    LOG.info("Start collective mapper.");
    long startTime = System.currentTimeMillis();
    List<String> pointFiles = new ArrayList<String>();
    while (reader.nextKeyValue()) {
      String key = reader.getCurrentKey();
      String value = reader.getCurrentValue();
      LOG.info("Key: " + key + ", Value: " + value);
      pointFiles.add(value);
    }
    Configuration conf = context.getConfiguration();
    runKmeans(pointFiles, conf, context);
    LOG.info("Total iterations in master view: "
      + (System.currentTimeMillis() - startTime));
  }

  private void runKmeans(List<String> fileNames, Configuration conf,
    Context context) throws IOException {
    // -----------------------------------------------------------------------------
    // Load centroids
    long startTime = System.currentTimeMillis();
    ArrTable<DoubleArray, DoubleArrPlus> cenTable = new ArrTable<DoubleArray, DoubleArrPlus>(
      0, DoubleArray.class, DoubleArrPlus.class);
    if (this.isMaster()) {
      Map<Integer, DoubleArray> cenParMap = createCenParMap(numCentroids,
        numCenPartitions, cenVecSize, this.getResourcePool());
      loadCentroids(cenParMap, vectorSize, conf.get(KMeansConstants.CFILE),
        conf);
      addPartitionMapToTable(cenParMap, cenTable);
    }
    long endTime = System.currentTimeMillis();
    LOG.info("Load centroids (ms): " + (endTime - startTime));
    // ------------------------------------------------------------------------------
    // Bcast centroids
    startTime = System.currentTimeMillis();
    bcastCentroids(cenTable, numCenPartitions);
    endTime = System.currentTimeMillis();
    LOG.info("Bcast centroids (ms): " + (endTime - startTime));
    // ---------------------------------------------------------------------------
    // Load data points
    // Use less threads to load data
    startTime = System.currentTimeMillis();
    List<DoubleArray> pointPartitions = doTasks(fileNames, "load-data-points",
      new PointLoadTask(pointsPerFile, vectorSize, conf), numMaxThreads);
    endTime = System.currentTimeMillis();
    LOG.info("File read (ms): " + (endTime - startTime));
    // -------------------------------------------------------------------------------------
    // For iterations
    ArrPartition<DoubleArray>[] cenPartitions = null;
    AtomicLong appKernelTime = new AtomicLong(0);
    for (int i = 0; i < iteration; i++) {
      LOG.info("Iteration: " + i);
      // -----------------------------------------------------------------
      // Calculate centroids dot prodcut c^2/2
      cenPartitions = cenTable.getPartitions();
      startTime = System.currentTimeMillis();
      doTasks(cenPartitions, "centroids-dot-product", new CenDotProductTask(
        cenVecSize), numMaxThreads);
      endTime = System.currentTimeMillis();
      LOG.info("Dot-product centroids (ms): " + (endTime - startTime));
      // Calculate new centroids
      startTime = System.currentTimeMillis();
      List<Map<Integer, DoubleArray>> localCenParMapList = doTasks(
        pointPartitions, "centroids-calc", new CenCalcTask(cenPartitions,
          numCentroids, numCenPartitions, vectorSize, this.getResourcePool(),
          context, appKernelTime), numMaxThreads);
      Set<Map<Integer, DoubleArray>> localCenParMapSet = new HashSet<Map<Integer, DoubleArray>>(
        localCenParMapList);
      localCenParMapList = null;
      endTime = System.currentTimeMillis();
      LOG.info("Calculate centroids (ms): " + (endTime - startTime));
      // -----------------------------------------------------------------
      // Merge localCenParMaps to cenPartitions
      // localCenParMaps uses thread local objects
      // we convert list to set to avoid duplicates
      startTime = System.currentTimeMillis();
      LOG.info("Local centroid data maps: " + localCenParMapSet.size());
      doTasks(cenPartitions, "centroids-merge", new CenMergeTask(
        localCenParMapSet), numMaxThreads);
      localCenParMapSet = null;
      endTime = System.currentTimeMillis();
      LOG.info("Merge centroids (ms): " + (endTime - startTime));
      // -----------------------------------------------------------------
      // Allreduce
      context.progress();
      ArrTable<DoubleArray, DoubleArrPlus> newCenTable = new ArrTable<DoubleArray, DoubleArrPlus>(
        i, DoubleArray.class, DoubleArrPlus.class);
      // Old table is altered during allreduce, ignore it.
      // Allreduce: regroup-combine-aggregate(reduce)-allgather.
      // Old table is at the state after regroup-combine-aggregate,
      // but new table is after allgather.
      try {
        startTime = System.currentTimeMillis();
        allreduce(cenTable, newCenTable, new Double2DArrAvg(cenVecSize));
        endTime = System.currentTimeMillis();
        LOG.info("Allreduce time (ms): " + (endTime - startTime));
      } catch (Exception e) {
        LOG.error("Fail to do allreduce.", e);
        throw new IOException(e);
      }
      cenTable = newCenTable;
      newCenTable = null;
      context.progress();
      logMemUsage();
      logGCTime();
    }
    // Write out new table
    if (this.isMaster()) {
      // LOG.info("App kernel time (nano seconds): " + appKernelTime.get());
      LOG.info("Start to write out centroids.");
      startTime = System.currentTimeMillis();
      storeCentroids(conf, cenTable, cenVecSize, jobID + 1);
      endTime = System.currentTimeMillis();
      LOG.info("Store centroids time (ms): " + (endTime - startTime));
    }
    // Clean all the references
    cenPartitions = cenTable.getPartitions();
    releasePartitions(cenPartitions);
    cenPartitions = null;
  }

  /**
   * Less parameters, better.
   * 
   * @param numCentroids
   * @param numCenPartitions
   * @param cenVecSize
   * @param resourcePool
   * @return
   */
  static Map<Integer, DoubleArray> createCenParMap(int numCentroids,
    int numCenPartitions, int cenVecSize, ResourcePool resourcePool) {
    int cenParSize = numCentroids / numCenPartitions;
    int cenRest = numCentroids % numCenPartitions;
    Map<Integer, DoubleArray> cenParMap = new Int2ObjectAVLTreeMap<DoubleArray>();
    double[] doubles = null;
    int size = 0;
    for (int i = 0; i < numCenPartitions; i++) {
      DoubleArray doubleArray = new DoubleArray();
      if (cenRest > 0) {
        size = (cenParSize + 1) * cenVecSize;
        doubles = resourcePool.getDoubleArrayPool().getArray(size);
        doubleArray.setArray(doubles);
        doubleArray.setSize(size);
        cenRest--;
      } else if (cenParSize > 0) {
        size = cenParSize * cenVecSize;
        doubles = resourcePool.getDoubleArrayPool().getArray(size);
        doubleArray.setArray(doubles);
        doubleArray.setSize(size);
      } else {
        // cenParSize <= 0
        break;
      }
      Arrays.fill(doubles, 0);
      cenParMap.put(i, doubleArray);
    }
    return cenParMap;
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
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    double[] cData;
    int start;
    int size;
    int collen = vectorSize + 1;
    String[] curLine = null;
    int curPos = 0;
    for (DoubleArray array : cenDataMap.values()) {
      cData = array.getArray();
      start = array.getStart();
      size = array.getSize();
      for (int i = start; i < (start + size); i++) {
        // Don't set the first element in each row
        if (i % collen != 0) {
          // cData[i] = in.readDouble();
          if (curLine == null || curPos == curLine.length - 1) {
            curLine = br.readLine().split(" ");
            curPos = 0;
          } else {
            curPos++;
          }
          cData[i] = Double.parseDouble(curLine[curPos]);
        }
      }
    }
    br.close();
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

  /**
   * Broadcast centroids data in partitions
   * 
   * @param table
   * @param numPartitions
   * @throws IOException
   */
  private <T, A extends Array<T>, C extends ArrCombiner<A>> void bcastCentroids(
    ArrTable<A, C> table, int numTotalPartitions) throws IOException {
    boolean success = true;
    try {
      success = arrTableBcastTotalKnown(table, numTotalPartitions);
    } catch (Exception e) {
      LOG.error("Fail to bcast.", e);
    }
    if (!success) {
      throw new IOException("Fail to bcast");
    }
  }

  private void releasePartitions(ArrPartition<DoubleArray>[] partitions) {
    for (int i = 0; i < partitions.length; i++) {
      this.getResourcePool().getDoubleArrayPool()
        .releaseArrayInUse(partitions[i].getArray().getArray());
    }
  }

  private static void storeCentroids(Configuration configuration,
    ArrTable<DoubleArray, DoubleArrPlus> cenTable, int cenVecSize, int jobID)
    throws IOException {
    String cFile = configuration.get(KMeansConstants.CFILE);
    Path cPath = new Path(cFile.substring(0, cFile.lastIndexOf("_") + 1)
      + jobID);
    LOG.info("centroids path: " + cPath.toString());
    FileSystem fs = FileSystem.get(configuration);
    fs.delete(cPath, true);
    FSDataOutputStream out = fs.create(cPath);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
    ArrPartition<DoubleArray> partitions[] = cenTable.getPartitions();
    int linePos = 0;
    for (ArrPartition<DoubleArray> partition : partitions) {
      for (int i = 0; i < partition.getArray().getSize(); i++) {
        linePos = i % cenVecSize;
        if (linePos == (cenVecSize - 1)) {
          bw.write(partition.getArray().getArray()[i] + "\n");
        } else if (linePos > 0) {
          // Every row with vectorSize + 1 length, the first one is a count,
          // ignore it in output
          bw.write(partition.getArray().getArray()[i] + " ");
        }
      }
    }
    bw.flush();
    bw.close();
    // out.flush();
    // out.sync();
    // out.close();
  }
}
