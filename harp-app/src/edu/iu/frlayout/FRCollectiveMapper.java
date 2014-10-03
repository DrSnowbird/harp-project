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

package edu.iu.frlayout;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CollectiveMapper;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.graph.EdgePartition;
import edu.iu.harp.graph.EdgeTable;
import edu.iu.harp.graph.EdgeVal;
import edu.iu.harp.graph.IntVertexID;
import edu.iu.harp.graph.LongVertexID;
import edu.iu.harp.graph.NullEdgeVal;
import edu.iu.harp.graph.OutEdgeTable;
import edu.iu.harp.graph.VertexID;
import edu.iu.harp.graph.vtx.IntFltArrVtxPartition;
import edu.iu.harp.graph.vtx.IntFltArrVtxTable;
import edu.iu.harp.graph.vtx.LongDblArrVtxPartition;
import edu.iu.harp.graph.vtx.LongDblArrVtxTable;
import edu.iu.harp.util.Int2ObjectReuseHashMap;
import edu.iu.harp.util.Long2ObjectReuseHashMap;

public class FRCollectiveMapper extends
  CollectiveMapper<String, String, Object, Object> {

  private String layoutFile;
  private int iteration;
  private int totalVtx;
  private int numMaps;
  private int partitionPerWorker;
  private double area;
  private double maxDelta;
  private double coolExp;
  private double repulseRad;
  private double k;
  private double ks;
  private int tableID;

  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    layoutFile = conf.get(FRConstants.LAYOUT_FILE);
    totalVtx = conf.getInt(FRConstants.TOTAL_VTX, 10);
    numMaps = conf.getInt(FRConstants.NUM_MAPS, 3);
    partitionPerWorker = conf.getInt(FRConstants.PARTITION_PER_WORKER, 8);
    iteration = conf.getInt(FRConstants.ITERATION, 1);
    area = totalVtx * totalVtx;
    maxDelta = totalVtx;
    coolExp = 1.5f;
    repulseRad = area * totalVtx;
    k = Math.sqrt(area / totalVtx);
    ks = totalVtx;
    tableID = 0;
  }

  /**
   * The partition layout should be the same between outEdgeTable, sgDisps, and
   * allGraphLayout. Table is a presentation of a data set across the whole
   * distributed environment The table ID is unique, is a reference of the table
   * A table in local process holds part of the partitions of the whole data
   * set. Edge table 0 contains partition 0, 1, 2, 3, 4, 5 but table 0 on
   * worker0 contains 0, 1, 2, table 1 on worker1 contains 3, 4, 5,
   */
  @Override
  public void mapCollective(KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    // Graph definition

    LOG.info("Total vtx count: " + totalVtx);
    int numParPerWorker = partitionPerWorker;
    LOG.info("Partition per worker: " + numParPerWorker);
    int maxNumPartitions = numParPerWorker * numMaps;
    int vtxPerPartition = totalVtx / maxNumPartitions;
    // Load / Generate out-edge table
    // outEdgeTable is hashed based on (sourceVertexID % totalPartitions)
    OutEdgeTable<IntVertexID, NullEdgeVal> outEdgeTable = new OutEdgeTable<IntVertexID, NullEdgeVal>(
      getNextTableID(), maxNumPartitions, 1000000, IntVertexID.class,
      NullEdgeVal.class, this.getResourcePool());
    // generateSubGraphEdges(outEdgeTable);
    while (reader.nextKeyValue()) {
      LOG.info("Load file: " + reader.getCurrentValue());
      loadSubGraphEdges(outEdgeTable, reader.getCurrentValue(),
        context.getConfiguration());
    }
    // Regroup generated edges
    try {
      // LOG.info("PRINT EDGE TABLE START BEFORE REGROUP");
      // printEdgeTable(outEdgeTable);
      // LOG.info("PRINT EDGE TABLE END BEFORE REGROUP");
      regroupEdges(outEdgeTable);
      // LOG.info("PRINT EDGE TABLE START AFTER REGROUP");
      // printEdgeTable(outEdgeTable);
      // LOG.info("PRINT EDGE TABLE END AFTER REGROUP");
    } catch (Exception e) {
      LOG.error("Error when adding edges", e);
    }
    // Generate sgDisps
    // sgDisps is hashed base on (vertexID % totalPartitions)
    IntFltArrVtxTable sgDisps = new IntFltArrVtxTable(getNextTableID(),
      maxNumPartitions, vtxPerPartition, 2);
    generateSgDisps(sgDisps, outEdgeTable);
    int sgVtxSize = 0;
    for (IntFltArrVtxPartition partition : sgDisps.getPartitions()) {
      sgVtxSize = sgVtxSize + partition.getVertexMap().size();
    }
    LOG.info("number of sg partitions: " + sgDisps.getNumPartitions()
      + ", number of vertices on this worker: " + sgVtxSize);
    // Load / Generate graph layout
    // allGraphLayout is hashed base on (vertexID % totalPartitions)
    IntFltArrVtxTable allGraphLayout = new IntFltArrVtxTable(getNextTableID(),
      maxNumPartitions, vtxPerPartition, 2);
    bcastAllGraphLayout(allGraphLayout, this.layoutFile,
      context.getConfiguration());
    masterBarrier();
    int totalVtxPartitions = getTotalVtxPartitions(sgDisps, getResourcePool());
    LOG.info("totalVtxPartitions: " + totalVtxPartitions);
    // Start iterations
    double t;
    IntFltArrVtxTable newGraphLayout = null;
    long start = System.currentTimeMillis();
    long task1Start = 0;
    long task1End = 0;
    long task2Start = 0;
    long task2End = 0;
    long task3Start = 0;
    long task3End = 0;
    long collstart = 0;
    long collend = 0;
    // int numThreads = Runtime.getRuntime().availableProcessors();
    int numThreads = numParPerWorker;
    IntFltArrVtxPartition[] glPartitions = null;
    LOG.info("Num Threads: " + numThreads);
    for (int i = 0; i < iteration; i++) {
      resetSgDisps(sgDisps);
      // Calculate repulsive forces and displacements
      task1Start = System.currentTimeMillis();
      // Initialize entry sets
      glPartitions = getGraphLayoutPartitions(allGraphLayout);
      doTasks(sgDisps.getPartitions(), "FR-task-1", new FRTask1(allGraphLayout,
        glPartitions, ks, area), numThreads);
      task1End = System.currentTimeMillis();
      // Calculate attractive forces and displacements
      task2Start = System.currentTimeMillis();
      doTasks(outEdgeTable.getPartitions(), "FR-task-2", new FRTask2(
        allGraphLayout, sgDisps, k, area), numThreads);
      task2End = System.currentTimeMillis();
      // Move the points with displacements limited by temperature
      task3Start = System.currentTimeMillis();
      t = maxDelta * Math.pow((iteration - i) / (double) iteration, coolExp);
      newGraphLayout = new IntFltArrVtxTable(getNextTableID(),
        maxNumPartitions, vtxPerPartition, 2);
      List<IntFltArrVtxPartition> newGlPartitions = doTasks(
        sgDisps.getPartitions(), "FR-task-3", new FRTask3(allGraphLayout, t),
        numThreads);
      for (IntFltArrVtxPartition partition : newGlPartitions) {
        newGraphLayout.addPartition(partition);
      }
      // Release other partitions in allGraphLayout which is not used
      for (IntFltArrVtxPartition partition : allGraphLayout.getPartitions()) {
        if (newGraphLayout.getPartition(partition.getPartitionID()) == null) {
          this.getResourcePool().getWritableObjectPool()
            .releaseWritableObjectInUse(partition);
        }
      }
      allGraphLayout = null;
      task3End = System.currentTimeMillis();
      collstart = System.currentTimeMillis();
      allgatherVtxTotalKnown(newGraphLayout, totalVtxPartitions);
      collend = System.currentTimeMillis();
      LOG.info("Total time in iteration. FR1: " + (task1End - task1Start)
        + " FR2: " + (task2End - task2Start) + " FR3: "
        + (task3End - task3Start) + " Allgather: " + (collend - collstart));
      allGraphLayout = newGraphLayout;
      newGraphLayout = null;
      // if (i % 1 == 0) {
      // context.progress();
      // }
      // Check received data size
      int size = 0;
      for (IntFltArrVtxPartition partition : allGraphLayout.getPartitions()) {
        size = size + partition.getVertexMap().size();
      }
      // LOG.info("Expected size: " + this.totalVtx + ", Real size: " + size);
      if (size != this.totalVtx) {
        throw new IOException("all gather fails");
      }
    }
    long end = System.currentTimeMillis();
    LOG.info("Total time on iterations: " + (end - start));
    storeGraphLayout(allGraphLayout, this.layoutFile + "_out",
      context.getConfiguration());
  }

  private IntFltArrVtxPartition[] getGraphLayoutPartitions(
    IntFltArrVtxTable allGraphLayout) {
    // Avoid repeating initialization of entryset on maps in parallel
    // processing.
    IntFltArrVtxPartition[] glPartitions = allGraphLayout.getPartitions();
    for (int i = 0; i < glPartitions.length; i++) {
      glPartitions[i].getVertexMap().int2ObjectEntrySet();
    }
    return glPartitions;
  }

  private int getTotalVtxPartitions(IntFltArrVtxTable table,
    ResourcePool resourcePool) {
    // Use allgather one to get the total number of vertex partitions
    ArrTable<DoubleArray, DoubleArrPlus> arrTable = new ArrTable<DoubleArray, DoubleArrPlus>(
      getNextTableID(), DoubleArray.class, DoubleArrPlus.class);
    // Create DoubleArray
    // Get double[1]
    double[] vals = resourcePool.getDoubleArrayPool().getArray(1);
    vals[0] = table.getNumPartitions();
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
        resourcePool.getDoubleArrayPool().releaseArrayInUse(inArray.getArray());
      }
    } catch (Exception e) {
      LOG.error("Fail to add partition to table", e);
      return 0;
    }
    try {
      allgatherOne(arrTable);
    } catch (Exception e) {
      LOG.error("Fail to do allreduce one.", e);
      return 0;
    }
    DoubleArray outArray = arrTable.getPartitions()[0].getArray();
    int totalParRecv = (int) outArray.getArray()[0];
    resourcePool.getDoubleArrayPool().releaseArrayInUse(outArray.getArray());
    return totalParRecv;
  }

  private void storeGraphLayout(IntFltArrVtxTable allGraphLayout,
    String fileName, Configuration configuration) throws IOException {
    if (this.isMaster()) {
      Path gPath = new Path(fileName);
      LOG.info("centroids path: " + gPath.toString());
      FileSystem fs = FileSystem.get(configuration);
      fs.delete(gPath, true);
      FSDataOutputStream out = fs.create(gPath);
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
      float[] fltArr = null;
      // ID is between 0 and totalVtx
      for (int i = 0; i < this.totalVtx; i++) {
        fltArr = allGraphLayout.getVertexVal(i);
        for (int j = 0; j < fltArr.length; j++) {
          bw.write(fltArr[j] + " ");
        }
        bw.newLine();
      }
      bw.flush();
      bw.close();
    }
  }

  private void resetSgDisps(IntFltArrVtxTable sgDisps) {
    IntFltArrVtxPartition[] partitions = sgDisps.getPartitions();
    Int2ObjectReuseHashMap<float[]> map = null;
    for (IntFltArrVtxPartition partition : partitions) {
      map = partition.getVertexMap();
      for (Int2ObjectMap.Entry<float[]> entry : map.int2ObjectEntrySet()) {
        Arrays.fill(entry.getValue(), 0);
      }
    }
  }

  private static void printVtxTable(LongDblArrVtxTable vtxTable) {
    LongDblArrVtxPartition[] vtxPartitions = vtxTable.getPartitions();
    StringBuffer buffer = new StringBuffer();
    for (LongDblArrVtxPartition partition : vtxPartitions) {
      for (Long2ObjectReuseHashMap.Entry<double[]> entry : partition
        .getVertexMap().long2ObjectEntrySet()) {
        for (int i = 0; i < entry.getValue().length; i++) {
          buffer.append(entry.getValue()[i]);
        }
        LOG.info("Patition: " + partition.getPartitionID() + " "
          + entry.getLongKey() + " " + buffer);
        buffer.delete(0, buffer.length());
      }
    }
  }

  private static <I extends VertexID, E extends EdgeVal, T extends EdgeTable<I, E>> void printEdgeTable(
    T edgeTable) {
    LongVertexID sourceID;
    LongVertexID targetID;
    for (EdgePartition<I, E> partition : edgeTable.getPartitions()) {
      while (partition.nextEdge()) {
        sourceID = (LongVertexID) partition.getCurSourceID();
        targetID = (LongVertexID) partition.getCurTargetID();
        LOG.info("Partiiton ID: " + partition.getPartitionID() + ", Edge: "
          + sourceID.getVertexID() + "->" + targetID.getVertexID());
      }
      partition.defaultReadPos();
    }
  }

  public void generateSubGraphEdges(
    OutEdgeTable<LongVertexID, NullEdgeVal> outEdgeTable) {
    int edgeCountPerWorker = 1;
    long source;
    long target;
    Random random = new Random(this.getWorkerID());
    for (int i = 0; i < edgeCountPerWorker; i++) {
      target = random.nextInt(totalVtx);
      do {
        source = random.nextInt(totalVtx);
      } while (source == target);
      LOG.info("Edge: " + source + "->" + target);
      outEdgeTable.addEdge(new LongVertexID(source), new NullEdgeVal(),
        new LongVertexID(target));
    }
    // Regroup generated edges
    try {
      LOG.info("PRINT EDGE TABLE START BEFORE REGROUP");
      printEdgeTable(outEdgeTable);
      LOG.info("PRINT EDGE TABLE END BEFORE REGROUP");
      regroupEdges(outEdgeTable);
      LOG.info("PRINT EDGE TABLE START AFTER REGROUP");
      printEdgeTable(outEdgeTable);
      LOG.info("PRINT EDGE TABLE END AFTER REGROUP");
    } catch (Exception e) {
      LOG.error("Error when adding edges", e);
    }
  }

  public void loadSubGraphEdges(
    OutEdgeTable<IntVertexID, NullEdgeVal> outEdgeTable, String fileName,
    Configuration configuration) throws IOException {
    Path gPath = new Path(fileName);
    LOG.info("adjacency list path: " + gPath.toString());
    FileSystem fs = FileSystem.get(configuration);
    FSDataInputStream input = fs.open(gPath);
    BufferedReader br = new BufferedReader(new InputStreamReader(input));
    IntVertexID sourceID = new IntVertexID();
    IntVertexID targetID = new IntVertexID();
    NullEdgeVal edgeVal = new NullEdgeVal();
    String[] vids = null;
    String line = br.readLine();
    while (line != null) {
      line = line.trim();
      if (line.length() > 0) {
        vids = line.split("\t");
        sourceID.setVertexID(Integer.parseInt(vids[0]));
        for (int i = 1; i < vids.length; i++) {
          targetID.setVertexID(Integer.parseInt(vids[i]));
          outEdgeTable.addEdge(sourceID, edgeVal, targetID);
        }
      }
      line = br.readLine();
    }
    br.close();
  }

  public void generateSgDisps(IntFltArrVtxTable sgDisps,
    OutEdgeTable<IntVertexID, NullEdgeVal> outEdgeTable) {
    float[] vtxVal = new float[2];
    IntVertexID vtxID = null;
    try {
      for (EdgePartition<IntVertexID, NullEdgeVal> partition : outEdgeTable
        .getPartitions()) {
        while (partition.nextEdge()) {
          vtxID = partition.getCurSourceID();
          sgDisps.initVertexVal(vtxID.getVertexID(), vtxVal);
        }
        partition.defaultReadPos();
      }
    } catch (Exception e) {
      LOG.error("Error when generating sgDisps", e);
    }
  }

  public void bcastAllGraphLayout(IntFltArrVtxTable allGraphLayout,
    String fileName, Configuration configuration) throws IOException {
    if (this.isMaster()) {
      // gnereateAllGraphLayout(allGraphLayout);
      LOG.info("Load layout: " + fileName);
      try {
        loadAllGraphLayout(allGraphLayout, fileName, configuration);
      } catch (Exception e) {
        LOG.error("Fail in Loading layout: " + fileName, e);
        throw (e);
      }
      // printVtxTable(allGraphLayout);
    }
    structTableBcast(allGraphLayout);
    int size = 0;
    for (IntFltArrVtxPartition partition : allGraphLayout.getPartitions()) {
      size = size + partition.getVertexMap().size();
    }
    LOG.info("Expected size: " + this.totalVtx + ", Real size: " + size);
    if (size != this.totalVtx) {
      throw new IOException("Bcast fails");
    }
  }

  private void gnereateAllGraphLayout(LongDblArrVtxTable allGraphLayout) {
    double[] vtxVal = new double[2];
    for (int i = 0; i < this.totalVtx; i++) {
      allGraphLayout.initVertexVal(i, vtxVal);
    }
  }

  private void loadAllGraphLayout(IntFltArrVtxTable allGraphLayout,
    String fileName, Configuration configuration) throws IOException {
    Path lPath = new Path(fileName);
    FileSystem fs = FileSystem.get(configuration);
    FSDataInputStream input = fs.open(lPath);
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    int numData = 0;
    int vecLen = 2;
    String inputLine = reader.readLine();
    if (inputLine != null) {
      numData = Integer.parseInt(inputLine);
    } else {
      throw new IOException("First line = number of rows is null");
    }
    inputLine = reader.readLine();
    if (inputLine != null) {
      vecLen = Integer.parseInt(inputLine);
    } else {
      throw new IOException("Second line = size of the vector is null");
    }
    if (allGraphLayout.getArrLen() != vecLen) {
      throw new IOException("Table doesn't match with the input.");
    }
    float[] vtxVal = new float[vecLen];
    String[] vecVals = null;
    int numRecords = 0;
    // Line No (numRecords) is the vertex ID
    while ((inputLine = reader.readLine()) != null) {
      vecVals = inputLine.split(" ");
      if (vecLen != vecVals.length) {
        throw new IOException("Vector length did not match at line "
          + numRecords);
      }
      for (int i = 0; i < vecLen; i++) {
        vtxVal[i] = Float.valueOf(vecVals[i]);
      }
      if (!allGraphLayout.initVertexVal(numRecords, vtxVal)) {
        throw new IOException("Fail to load allGraphLayout");
      }
      numRecords++;
    }
    reader.close();
    input.close();
  }

  private int getNextTableID() {
    return tableID++;
  }
}
