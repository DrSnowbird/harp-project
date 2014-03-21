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

package edu.iu.pagerank;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CollectiveMapper;

import edu.iu.harp.graph.EdgePartition;
import edu.iu.harp.graph.InEdgeTable;
import edu.iu.harp.graph.IntMsgVal;
import edu.iu.harp.graph.LongVertexID;
import edu.iu.harp.graph.MsgPartition;
import edu.iu.harp.graph.MsgTable;
import edu.iu.harp.graph.NullEdgeVal;
import edu.iu.harp.graph.vtx.LongDblVtxPartition;
import edu.iu.harp.graph.vtx.LongDblVtxTable;
import edu.iu.harp.graph.vtx.LongIntVtxPartition;
import edu.iu.harp.graph.vtx.LongIntVtxTable;

public class PRMultiThreadMapper extends
  CollectiveMapper<String, String, Object, Object> {

  private int iteration;
  private int totalVtx;
  private int numMaps;
  private int partitionPerWorker;

  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    totalVtx = conf.getInt(PRConstants.TOTAL_VTX, 10);
    numMaps = conf.getInt(PRConstants.NUM_MAPS, 3);
    partitionPerWorker = conf.getInt(PRConstants.PARTITION_PER_WORKER, 8);
    iteration = conf.getInt(PRConstants.ITERATION, 1);
  }

  @Override
  public void mapCollective(KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    try {
      doPageRank(reader, context);
    } catch (Exception e) {
      LOG.error("Error when doing page rank", e);
    }
  }

  /**
   * The partition layout should be the same between inEdgeTable, ldVtxTable and
   * liVtxTable. Table is a presentation of a data set across the whole
   * distributed environment The table ID is unique, is a reference of the table
   * A table in local process holds part of the partitions of the whole data
   * set. Edge table 0 contains partition 0, 1, 2, 3, 4, 5 but table 0 on
   * worker0 contains 0, 1, 2, table 1 on worker1 contains 3, 4, 5,
   */
  private void doPageRank(KeyValReader reader, Context context)
    throws Exception {
    int numThreads = 2;
    LOG.info("Total vtx count: " + totalVtx);
    LOG.info("Partition per worker: " + partitionPerWorker);
    int maxNumPartitions = partitionPerWorker * numMaps;
    int vtxPerPartition = totalVtx / maxNumPartitions;
    // Load in-edge table
    // inEdgeTable is hashed based on (targetVertexID % totalPartitions)
    InEdgeTable<LongVertexID, NullEdgeVal> inEdgeTable = new InEdgeTable<LongVertexID, NullEdgeVal>(
      0, maxNumPartitions, 1000000, LongVertexID.class, NullEdgeVal.class,
      this.getResourcePool());
    while (reader.nextKeyValue()) {
      LOG.info("Load file: " + reader.getCurrentValue());
      loadSubGraphEdges(inEdgeTable, reader.getCurrentValue(),
        context.getConfiguration());
    }
    // Regroup edges based on partition ID
    try {
      regroupEdges(inEdgeTable);
    } catch (Exception e) {
      LOG.error("Error when adding edges", e);
      return;
    }
    // Generate vertex table
    LongDblVtxTable ldVtxTable = new LongDblVtxTable(1, maxNumPartitions,
      vtxPerPartition);
    List<LongDblVtxPartition> ldVtxPartitions = doTasks(
      new ObjectArrayList<EdgePartition<LongVertexID, NullEdgeVal>>(
        inEdgeTable.getPartitions()), "init-val-task-1", new InitValTask1(
        vtxPerPartition, totalVtx, this.getResourcePool()), numThreads);
    for (LongDblVtxPartition partition : ldVtxPartitions) {
      ldVtxTable.addPartition(partition);
    }
    // Generate message for out-edge count
    MsgTable<LongVertexID, IntMsgVal> msgTable = new MsgTable<LongVertexID, IntMsgVal>(
      2, maxNumPartitions, 1000000, LongVertexID.class, IntMsgVal.class,
      this.getResourcePool());
    LongVertexID sourceID;
    IntMsgVal iMsgVal = new IntMsgVal(1);
    for (EdgePartition<LongVertexID, NullEdgeVal> partition : inEdgeTable
      .getPartitions()) {
      while (partition.nextEdge()) {
        sourceID = partition.getCurSourceID();
        msgTable.addMsg(sourceID, iMsgVal);
      }
      partition.defaultReadPos();
    }
    // All-to-all communication, moves message partition to the place
    // where the vertex partition locate
    allMsgToAllVtx(msgTable, ldVtxTable);
    // Another vertex table for out edge count
    LongIntVtxTable liVtxTable = new LongIntVtxTable(2, maxNumPartitions,
      vtxPerPartition);
    // Process msg table
    List<LongIntVtxPartition> livtxPartitions = doTasks(
      new ObjectArrayList<MsgPartition<LongVertexID, IntMsgVal>>(
        msgTable.getPartitions()), "init-val-task-2", new InitValTask2(
        ldVtxTable, vtxPerPartition, totalVtx, this.getResourcePool()),
      numThreads);
    for (LongIntVtxPartition partition : livtxPartitions) {
      liVtxTable.addPartition(partition);
    }
    LongDblVtxTable newLdVtxTable = null;
    for (int i = 0; i < iteration; i++) {
      // Allgather page-rank value
      allgatherVtx(ldVtxTable);
      LOG.info("Expected size: " + this.totalVtx + ", Real size: "
        + getVtxCount(ldVtxTable));
      newLdVtxTable = new LongDblVtxTable(4, maxNumPartitions, vtxPerPartition);
      List<LongDblVtxPartition> newLdVtxPartitions = doTasks(
        new ObjectArrayList<EdgePartition<LongVertexID, NullEdgeVal>>(
          inEdgeTable.getPartitions()), "calc-pr-task-1", new CalcPRTask1(
          ldVtxTable, vtxPerPartition, this.getResourcePool()), numThreads);
      for (LongDblVtxPartition partition : newLdVtxPartitions) {
        newLdVtxTable.addPartition(partition);
      }
      // Release
      LongDblVtxPartition[] vtxPartitions = ldVtxTable.getPartitions();
      for (LongDblVtxPartition partition : vtxPartitions) {
        this.getResourcePool().getWritableObjectPool()
          .releaseWritableObjectInUse(partition);
      }
      ldVtxTable = null;
      if (i < iteration - 1) {
        doTasks(
          new ObjectArrayList<LongIntVtxPartition>(liVtxTable.getPartitions()),
          "calc-pr-task-2", new CalcPRTask2(newLdVtxTable, vtxPerPartition,
            totalVtx, this.getResourcePool()), numThreads);
      }
      ldVtxTable = newLdVtxTable;

    }
    allgatherVtx(ldVtxTable);
    LOG.info("Expected size: " + this.totalVtx + ", Real size: "
      + getVtxCount(ldVtxTable));
    if (this.isMaster()) {
      printVtxTable(ldVtxTable);
    }
  }

  public void loadSubGraphEdges(
    InEdgeTable<LongVertexID, NullEdgeVal> inEdgeTable, String fileName,
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
          inEdgeTable.addEdge(sourceID, edgeVal, targetID);
        }
      }
      line = br.readLine();
    }
    br.close();
  }

  private static void printVtxTable(LongDblVtxTable vtxTable) {
    LongDblVtxPartition[] vtxPartitions = vtxTable.getPartitions();
    for (LongDblVtxPartition partition : vtxPartitions) {
      for (Long2DoubleMap.Entry entry : partition.getVertexMap()
        .long2DoubleEntrySet()) {
        LOG.info("Patition " + partition.getPartitionID() + ", ID "
          + entry.getLongKey() + ", Val " + entry.getDoubleValue());
      }
    }
  }

  private int getVtxCount(LongDblVtxTable table) {
    int size = 0;
    for (LongDblVtxPartition partition : table.getPartitions()) {
      size = size + partition.getVertexMap().size();
    }
    return size;
  }
}
