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

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.CollectiveMapper;

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

public class FRMapper extends
  CollectiveMapper<String, String, Object, Object> {

  private int iteration;
  private int totalVtx;
  private int numMaps;
  private double area;
  private double maxDelta;
  private double coolExp;
  private double repulseRad;
  private double k;
  private double ks;
  private Random r;

  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    totalVtx = conf.getInt(FRConstants.TOTAL_VTX, 10);
    numMaps = conf.getInt(FRConstants.NUM_MAPS, 3);
    iteration = conf.getInt(FRConstants.ITERATION, 1);
    area = totalVtx * totalVtx;
    maxDelta = totalVtx;
    coolExp = 1.5f;
    repulseRad = area * totalVtx;
    k = Math.sqrt(area / totalVtx);
    ks = totalVtx;
    r = new Random();
  }

  @Override
  public void mapCollective(KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    // Graph definition
    LOG.info("Total vtx count: " + totalVtx);
    int numParPerWorker = 2;
    int totalPartitions = numParPerWorker * numMaps;
    int vtxPerPartition = totalVtx / totalPartitions;
    int vtxPerWorker = vtxPerPartition * numParPerWorker;
    int edgeCountPerWorker = 1;
    // Load split data
    // Generate out-edge table
    OutEdgeTable<LongVertexID, NullEdgeVal> outEdgeTable = new OutEdgeTable<LongVertexID, NullEdgeVal>(
      0, totalPartitions, 1000000, LongVertexID.class, NullEdgeVal.class,
      this.getResourcePool());
    // Generate subGraphEdges
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
    // Generate sgDisps
    LongDblArrVtxTable sgDisps = new LongDblArrVtxTable(1, totalPartitions,
      vtxPerPartition, 2);
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
    // Load / Generate graph layout
    LongDblArrVtxTable allGraphLayout = new LongDblArrVtxTable(2,
      totalPartitions, vtxPerPartition, 2);
    if (this.isMaster()) {
      loadAllGraphLayout(allGraphLayout);
      printVtxTable(allGraphLayout);
    }
    // Broadcast
    structTableBcast(allGraphLayout);

    long sgVtxID;
    double[] sgDblArr = null;
    long glVtxID;
    double[] glDblArr1 = null;
    double[] glDblArr2 = null;
    double xd;
    double yd;
    double dedS;
    double ded;
    double rf;
    double af;
    double t;
    LongVertexID srcID = null;
    LongVertexID tgtID = null;
    LongDblArrVtxTable newGraphLayout = null;
    for (int i = 0; i < this.iteration; i++) {
      resetSgDisps(sgDisps);
      // Calculate repulsive forces and displacements
      for (LongDblArrVtxPartition sgPartition : sgDisps.getPartitions()) {
        Long2ObjectOpenHashMap<double[]> sgMap = sgPartition.getVertexMap();
        for (Long2ObjectMap.Entry<double[]> sgEntry : sgMap
          .long2ObjectEntrySet()) {
          sgVtxID = sgEntry.getLongKey();
          sgDblArr = sgEntry.getValue();
          for (LongDblArrVtxPartition glPartition : allGraphLayout
            .getPartitions()) {
            Long2ObjectOpenHashMap<double[]> glMap = glPartition.getVertexMap();
            for (Long2ObjectMap.Entry<double[]> glEntry : glMap
              .long2ObjectEntrySet()) {
              glVtxID = glEntry.getLongKey();
              glDblArr1 = glEntry.getValue();
              // ?
              if (sgVtxID == glVtxID) {
                continue;
              }
              glDblArr2 = allGraphLayout.getVertexVal(sgVtxID);
              xd = glDblArr2[0] - glDblArr1[0];
              yd = glDblArr2[1] - glDblArr1[1];
              dedS = xd * xd + yd * yd;
              ded = Math.sqrt(dedS);
              rf = 0;
              if (ded != 0) {
                /*
                 * xd /= ded; yd /= ded; rf = ks * (float)(1.0 / ded - dedS /
                 * repulseRad); disps[m][0] += xd * rf; disps[m][1] += yd * rf;
                 */
                rf = ks / dedS - ded / area;
                sgDblArr[0] += xd * rf;
                sgDblArr[1] += yd * rf;
              } else {
                xd = r.nextGaussian() * 0.1;
                yd = r.nextGaussian() * 0.1;
                rf = r.nextGaussian() * 0.1;
                sgDblArr[0] += xd * rf;
                sgDblArr[1] += yd * rf;
              }
            }
          }
        }
      }
      // Calculate attractive forces and displacements
      for (EdgePartition<LongVertexID, NullEdgeVal> partition : outEdgeTable
        .getPartitions()) {
        while (partition.nextEdge()) {
          if (srcID == null) {
            srcID = partition.getCurSourceID();
            glDblArr1 = allGraphLayout.getVertexVal(srcID.getVertexID());
          } else if (srcID.getVertexID() != partition.getCurSourceID()
            .getVertexID()) {
            srcID = partition.getCurSourceID();
            glDblArr1 = allGraphLayout.getVertexVal(srcID.getVertexID());
          }
          tgtID = partition.getCurTargetID();
          glDblArr2 = allGraphLayout.getVertexVal(tgtID.getVertexID());
          xd = glDblArr1[0] - glDblArr2[0];
          yd = glDblArr1[1] - glDblArr2[1];
          ded = Math.sqrt(xd * xd + yd * yd);
          af = 0;
          sgDblArr = sgDisps.getVertexVal(srcID.getVertexID());
          if (ded != 0) {
            /*
             * xd /= ded; yd /= ded; af = ded * ded / k; disps[m][0] -= xd * af;
             * disps[m][1] -= yd * af;
             */
            af = ded / k;
            sgDblArr[0] -= xd * af;
            sgDblArr[1] -= yd * af;
          } else {
            xd = r.nextGaussian() * 0.1;
            yd = r.nextGaussian() * 0.1;
            af = r.nextGaussian() * 0.1;
            sgDblArr[0] -= xd * af;
            sgDblArr[1] -= yd * af;
          }
        }
        partition.defaultReadPos();
      }
      // Move the points with displacements limited by temperature
      t = maxDelta
        * Math.pow((this.iteration - i) / (double) this.iteration, coolExp);

      newGraphLayout = new LongDblArrVtxTable(3, totalPartitions,
        vtxPerPartition, 2);

      for (LongDblArrVtxPartition sgPartition : sgDisps.getPartitions()) {
        Long2ObjectOpenHashMap<double[]> sgMap = sgPartition.getVertexMap();
        for (Long2ObjectMap.Entry<double[]> sgEntry : sgMap
          .long2ObjectEntrySet()) {
          sgVtxID = sgEntry.getLongKey();
          sgDblArr = sgEntry.getValue();
          ded = Math
            .sqrt(sgDblArr[0] * sgDblArr[0] + sgDblArr[1] * sgDblArr[1]);
          if (ded > t) {
            ded = t / ded;
            sgDblArr[0] *= ded;
            sgDblArr[1] *= ded;
          }
          glDblArr1 = allGraphLayout.getVertexVal(sgVtxID);
          glDblArr1[0] += sgDblArr[0];
          glDblArr1[1] += sgDblArr[1];
        }
      }
      for (int partitionID : sgDisps.getPartitionIDs()) {
        newGraphLayout.addPartition(allGraphLayout.getPartition(partitionID));
      }
      allGraphLayout = null;
      allgatherVtx(newGraphLayout);
      allGraphLayout = newGraphLayout;
      newGraphLayout = null;
    }
  }

  private void loadAllGraphLayout(LongDblArrVtxTable allGraphLayout) {
    double[] vtxVal = new double[2];
    for (int i = 0; i < this.totalVtx; i++) {
      allGraphLayout.initVertexVal(i, vtxVal);
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
}
