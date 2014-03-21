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

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

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

import edu.iu.harp.collective.CollCommWorker;
import edu.iu.harp.graph.EdgePartition;
import edu.iu.harp.graph.EdgeTable;
import edu.iu.harp.graph.EdgeVal;
import edu.iu.harp.graph.LongVertexID;
import edu.iu.harp.graph.NullEdgeVal;
import edu.iu.harp.graph.OutEdgeTable;
import edu.iu.harp.graph.VertexID;
import edu.iu.harp.graph.vtx.LongDblArrVtxPartition;
import edu.iu.harp.graph.vtx.LongDblArrVtxTable;
import edu.iu.harp.util.Long2ObjectOpenHashMap;

public class FRMultiThreadMapper extends
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
    OutEdgeTable<LongVertexID, NullEdgeVal> outEdgeTable = new OutEdgeTable<LongVertexID, NullEdgeVal>(
      0, maxNumPartitions, 1000000, LongVertexID.class, NullEdgeVal.class,
      this.getResourcePool());
    // generateSubGraphEdges(outEdgeTable);
    while (reader.nextKeyValue()) {
      LOG.info("Load file: " + reader.getCurrentValue());
      loadSubGraphEdges(outEdgeTable, reader.getCurrentValue(),
        context.getConfiguration());
    }
    // Regroup generated edges
    try {
      /*
       * LOG.info("PRINT EDGE TABLE START BEFORE REGROUP");
       * printEdgeTable(outEdgeTable);
       * LOG.info("PRINT EDGE TABLE END BEFORE REGROUP");
       */
      regroupEdges(outEdgeTable);
      /*
       * LOG.info("PRINT EDGE TABLE START AFTER REGROUP");
       * printEdgeTable(outEdgeTable);
       * LOG.info("PRINT EDGE TABLE END AFTER REGROUP");
       */
    } catch (Exception e) {
      LOG.error("Error when adding edges", e);
    }
    // Generate sgDisps
    // sgDisps is hashed base on (vertexID % totalPartitions)
    LongDblArrVtxTable sgDisps = new LongDblArrVtxTable(1, maxNumPartitions,
      vtxPerPartition, 2);
    generateSgDisps(sgDisps, outEdgeTable);
    // Load / Generate graph layout
    // allGraphLayout is hashed base on (vertexID % totalPartitions)
    LongDblArrVtxTable allGraphLayout = new LongDblArrVtxTable(2,
      maxNumPartitions, vtxPerPartition, 2);
    bcastAllGraphLayout(allGraphLayout, this.layoutFile,
      context.getConfiguration());
    // Start iterations
    double t;
    LongDblArrVtxTable newGraphLayout = null;
    long start = System.currentTimeMillis();
    long task1Start;
    long task1End;
    long task2Start;
    long task2End;
    long task3Start;
    long task3End;
    long collstart;
    long collend;
    int numThreads = Runtime.getRuntime().availableProcessors();
    LOG.info("Num Threads: " + numThreads);
    for (int i = 0; i < this.iteration; i++) {
      resetSgDisps(sgDisps);
      // Calculate repulsive forces and displacements
      task1Start = System.currentTimeMillis();
      CollCommWorker.doTasks(new ObjectArrayList<LongDblArrVtxPartition>(
        sgDisps.getPartitions()), "FR-task-1", new FRTask1(allGraphLayout, ks,
        area), numThreads);
      task1End = System.currentTimeMillis();
      // Calculate attractive forces and displacements
      task2Start = System.currentTimeMillis();
      CollCommWorker.doTasks(
        new ObjectArrayList<EdgePartition<LongVertexID, NullEdgeVal>>(
          outEdgeTable.getPartitions()), "FR-task-2", new FRTask2(
          allGraphLayout, sgDisps, k, area), numThreads);
      task2End = System.currentTimeMillis();
      // Move the points with displacements limited by temperature
      task3Start = System.currentTimeMillis();
      t = maxDelta
        * Math.pow((this.iteration - i) / (double) this.iteration, coolExp);
      newGraphLayout = new LongDblArrVtxTable(3, maxNumPartitions,
        vtxPerPartition, 2);
      List<LongDblArrVtxPartition> newGlPartitions = CollCommWorker.doTasks(
        new ObjectArrayList<LongDblArrVtxPartition>(sgDisps.getPartitions()),
        "FR-task-3", new FRTask3(allGraphLayout, t), numThreads);
      for (LongDblArrVtxPartition partition : newGlPartitions) {
        newGraphLayout.addPartition(partition);
      }
      // Release other partitions in allGraphLayout which is not used
      for (LongDblArrVtxPartition partition : allGraphLayout.getPartitions()) {
        if (newGraphLayout.getPartition(partition.getPartitionID()) == null) {
          this.getResourcePool().getWritableObjectPool()
            .releaseWritableObjectInUse(partition);
        }
      }
      task3End = System.currentTimeMillis();
      allGraphLayout = null;
      collstart = System.currentTimeMillis();
      allgatherVtx(newGraphLayout);
      collend = System.currentTimeMillis();
      LOG.info("Total time in iteration: " + (task1End - task1Start) + " "
        + (task2End - task2Start) + " " + (task3End - task3Start) + " "
        + (collend - collstart));
      allGraphLayout = newGraphLayout;
      newGraphLayout = null;
      if (this.iteration % 100 == 0) {
        context.progress();
      }
      // Check received data size
      int size = 0;
      for (LongDblArrVtxPartition partition : allGraphLayout.getPartitions()) {
        size = size + partition.getVertexMap().size();
      }
      LOG.info("Expected size: " + this.totalVtx + ", Real size: " + size);
      if (size != this.totalVtx) {
        throw new IOException("all gather fails");
      }
    }
    long end = System.currentTimeMillis();
    LOG.info("Total time on iterations: " + (end - start));
    storeGraphLayout(allGraphLayout, this.layoutFile + "_out",
      context.getConfiguration());
  }

  private void storeGraphLayout(LongDblArrVtxTable allGraphLayout,
    String fileName, Configuration configuration) throws IOException {
    if (this.isMaster()) {
      Path gPath = new Path(fileName);
      LOG.info("centroids path: " + gPath.toString());
      FileSystem fs = FileSystem.get(configuration);
      fs.delete(gPath, true);
      FSDataOutputStream out = fs.create(gPath);
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
      double[] dblArr = null;
      // ID is between 0 and totalVtx
      for (int i = 0; i < this.totalVtx; i++) {
        dblArr = allGraphLayout.getVertexVal(i);
        for (int j = 0; j < dblArr.length; j++) {
          bw.write(dblArr[j] + " ");
        }
        bw.newLine();
      }
      bw.flush();
      bw.close();
    }
  }

  private void resetSgDisps(LongDblArrVtxTable sgDisps) {
    LongDblArrVtxPartition[] partitions = sgDisps.getPartitions();
    Long2ObjectOpenHashMap<double[]> map;
    for (LongDblArrVtxPartition partition : partitions) {
      map = partition.getVertexMap();
      for (Long2ObjectMap.Entry<double[]> entry : map.long2ObjectEntrySet()) {
        Arrays.fill(entry.getValue(), 0);
      }
    }
  }

  private static void printVtxTable(LongDblArrVtxTable vtxTable) {
    LongDblArrVtxPartition[] vtxPartitions = vtxTable.getPartitions();
    StringBuffer buffer = new StringBuffer();
    for (LongDblArrVtxPartition partition : vtxPartitions) {
      for (Long2ObjectOpenHashMap.Entry<double[]> entry : partition
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
    OutEdgeTable<LongVertexID, NullEdgeVal> outEdgeTable, String fileName,
    Configuration configuration) throws IOException {
    Path gPath = new Path(fileName);
    LOG.info("centroids path: " + gPath.toString());
    FileSystem fs = FileSystem.get(configuration);
    FSDataInputStream input = fs.open(gPath);
    BufferedReader br = new BufferedReader(new InputStreamReader(input));
    LongVertexID sourceID = new LongVertexID();
    LongVertexID targetID = new LongVertexID();
    NullEdgeVal edgeVal = new NullEdgeVal();
    String[] vids = null;
    String line = br.readLine();
    while (line != null) {
      line = line.trim();
      if (line.length() > 0) {
        vids = line.split("\t");
        sourceID.setVertexID(Long.parseLong(vids[0]));
        for (int i = 1; i < vids.length; i++) {
          targetID.setVertexID(Long.parseLong(vids[i]));
          outEdgeTable.addEdge(sourceID, edgeVal, targetID);
        }
      }
      line = br.readLine();
    }
    br.close();
  }

  public void generateSgDisps(LongDblArrVtxTable sgDisps,
    OutEdgeTable<LongVertexID, NullEdgeVal> outEdgeTable) {
    double[] vtxVal = new double[2];
    LongVertexID vtxID;
    try {
      for (EdgePartition<LongVertexID, NullEdgeVal> partition : outEdgeTable
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

  public void bcastAllGraphLayout(LongDblArrVtxTable allGraphLayout,
    String fileName, Configuration configuration) throws IOException {
    if (this.isMaster()) {
      // gnereateAllGraphLayout(allGraphLayout);
      LOG.info("Load layout: " + fileName);
      try {
        loadAllGraphLayout(allGraphLayout, fileName, configuration);
      } catch (Exception e) {
        LOG.info("Fail in Loading layout: " + fileName);
      }
      // printVtxTable(allGraphLayout);
    }
    structTableBcast(allGraphLayout);
    int size = 0;
    for (LongDblArrVtxPartition partition : allGraphLayout.getPartitions()) {
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

  private void loadAllGraphLayout(LongDblArrVtxTable allGraphLayout,
    String fileName, Configuration configuration) throws IOException {
    Path lPath = new Path(fileName);
    FileSystem fs = FileSystem.get(configuration);
    FSDataInputStream input = fs.open(lPath);
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    int numData;
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
    double[] vtxVal = new double[vecLen];
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
        vtxVal[i] = Double.valueOf(vecVals[i]);
      }
      if (!allGraphLayout.initVertexVal(numRecords, vtxVal)) {
        throw new IOException("Fail to load allGraphLayout");
      }
      numRecords++;
    }
    reader.close();
    input.close();
  }
}
