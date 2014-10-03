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

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.CollectiveMapper;

import edu.iu.harp.arrpar.ArrCombiner;
import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class WDAMDSMapper extends
  CollectiveMapper<String, String, Object, Object> {
  private int numMapTasks;
  private String xFile;
  private String xOutFile;
  private String idsFile;
  private String labelsFile;
  private double threshold;
  private int d;
  private double alpha;
  private int n;
  private int cgIter;
  private int cgRealIter;
  private int maxNumThreads;
  private Configuration conf;
  private int tableID;
  private int totalPartitions;
  // Metrics;
  private long totalInitXTime;
  private long totalInitXCount;
  private long totalBCTime;
  private long totalBCCount;
  private long totalMMTime;
  private long totalMMCount;
  private long totalStressTime;
  private long totalStressCount;
  private long totalInnerProductTime;
  private long totalInnerProductCount;
  private long totalAllgatherTime;
  private long totalAllgatherSyncTime;
  private long totalAllgatherCount;
  private long totalAllgatherValTime;
  private long totalAllgatherValSyncTime;
  private long totalAllgatherValCount;

  public void setup(Context context) {
    conf = context.getConfiguration();
    numMapTasks = conf.getInt(MDSConstants.NUM_MAPS, 1);
    xFile = conf.get(MDSConstants.X_FILE_PATH, "");
    xOutFile = conf.get(MDSConstants.X_OUT_FILE_PATH, "");
    idsFile = conf.get(MDSConstants.IDS_FILE, "");
    labelsFile = conf.get(MDSConstants.LABELS_FILE, "");
    threshold = conf.getDouble(MDSConstants.THRESHOLD, 1);
    d = conf.getInt(MDSConstants.D, 1);
    alpha = conf.getDouble(MDSConstants.ALPHA, 0.1);
    // alpha = 0;
    n = conf.getInt(MDSConstants.N, 1);
    cgIter = conf.getInt(MDSConstants.CG_ITER, 1);
    maxNumThreads = conf.getInt(MDSConstants.NUM_THREADS, 8);
    LOG.info("Max number of threads: " + maxNumThreads);
    cgRealIter = 0;
    
    totalInitXTime = 0;
    totalInitXCount = 0;
    totalBCTime = 0;
    totalBCCount = 0;
    totalMMTime = 0;
    totalMMCount = 0;
    totalStressTime = 0;
    totalStressCount = 0;
    totalInnerProductTime = 0;
    totalInnerProductCount = 0;
    totalAllgatherTime = 0;
    totalAllgatherSyncTime = 0;
    totalAllgatherCount = 0;
    totalAllgatherValTime = 0;
    totalAllgatherValSyncTime = 0;
    totalAllgatherValCount = 0;
  }

  @Override
  public void mapCollective(KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    LOG.info("Start collective mapper.");
    List<String> partitionFiles = new ArrayList<String>();
    while (reader.nextKeyValue()) {
      String key = reader.getCurrentKey();
      String value = reader.getCurrentValue();
      LOG.info("Key: " + key + ", Value: " + value);
      partitionFiles.add(value);
    }
    context.progress();
    // There should be only one file
    try {
      runMDS(partitionFiles.get(0), context);
    } catch (Exception e) {
      LOG.error("Fail to run MDS.", e);
      throw new IOException(e);
    }
  }

  public void runMDS(String partitionFile, Context context) throws Exception {
    double startTime = System.currentTimeMillis();
    List<RowData> rowDataList = loadData(partitionFile, context);
    double endTime = System.currentTimeMillis();
    LOG.info("Load data took =" + (endTime - startTime) / 1000 + " Seconds.");
    logMemUsage();
    logGCTime();
    context.progress();
    // startTime = System.currentTimeMillis();
    // -----------------------------------------------------------------------------------
    final ResourcePool pool = this.getResourcePool();
    // Perform Initial Calculations
    // Average distance calculation
    double[] avgs = calculateAvgDistances(rowDataList, pool);
    // Print average distances
    double div = (double) n * (double) n - (double) n;
    double avgOrigDistance = avgs[0] / div;
    double sumOrigDistanceSquare = avgs[1];
    double avgOrigDistanceSquare = avgs[1] / div;
    double maxOrigDistance = avgs[2];
    this.getResourcePool().getDoubleArrayPool().releaseArrayInUse(avgs);
    LOG.info("N: " + n);
    LOG.info("AvgOrgDistance: " + avgOrigDistance);
    LOG.info("SumSquareOrgDistance: " + sumOrigDistanceSquare);
    LOG.info("MaxOrigDistance: " + maxOrigDistance);
    // ------------------------------------------------------------------------------------
    // Create X and allgather X
    ArrTable<DoubleArray, DoubleArrPlus> xTable = initializeX(d, rowDataList,
      this.getResourcePool());
    ArrPartition<DoubleArray>[] xPartitions = xTable.getPartitions();
    if (xPartitions.length != totalPartitions) {
      throw new Exception("Fail to initialize X.");
    }
    LOG.info("Initial X is generated.");
    // int[][] xRowIndex = new int[2][xPartitions.length];
    // CalcUtil.buildXRowIndex(xPartitions, d, xRowIndex);
    // -------------------------------------------------------------------------------------
    // Calculate initial stress value
    double tCur = 0;
    double preStress = calculateStress(rowDataList, xPartitions, tCur,
      sumOrigDistanceSquare, pool);
    LOG.info("Initial Stress: " + preStress);
    // ---------------------------------------------------------------------------------
    double diffStress = 10 * threshold; // Starting value
    double stress = 0;
    double qoR1 = 0;
    double qoR2 = 0;
    double tMax = maxOrigDistance / Math.sqrt(2.0 * d);
    double tMin = (0.01 * tMax < 0.01) ? 0.01 * tMax : 0.01;
    tCur = alpha * tMax;
    endTime = System.currentTimeMillis();
    LOG.info("Upto the loop took =" + (endTime - startTime) / 1000
      + " Seconds.");
    // ---------------------------------------------------------------------------------
    int iter = 0;
    int smacofRealIter = 0;
    while (tCur > tMin) {
      preStress = calculateStress(rowDataList, xPartitions, tCur,
        sumOrigDistanceSquare, pool);
      diffStress = threshold + 1.0;
      LOG.info("###############################");
      LOG.info("# T_Cur = " + tCur);
      LOG.info("###############################");
      while (diffStress >= threshold) {
        List<XRowData> bcList = calculateBC(rowDataList, xPartitions, tCur, pool);
        
        // Temporarily no release but forward to cg iterations
        // Release old X
        // releasePartitions(xPartitions);
        // xTable = null;
        // xPartitions = null;
        
        // Calculate new X
        // xTable = conjugateGradient(rowDataList, bcList, pool);
        xTable = conjugateGradient(rowDataList, bcList, xTable, pool);
        
        xPartitions = xTable.getPartitions();
        // Release bc
        for (XRowData bc : bcList) {
          pool.getDoubleArrayPool().releaseArrayInUse(bc.array.getArray());
        }
        bcList = null;
        stress = calculateStress(rowDataList, xPartitions, tCur,
          sumOrigDistanceSquare, pool);
        diffStress = preStress - stress;
        iter++;
        smacofRealIter++;
        if (iter == 1) {
          LOG.info("Iteration ## " + iter + " completed. " + threshold + " "
            + diffStress + " " + stress);
          context.progress();
        }
        if (iter % 10 == 0) {
          LOG.info("Iteration ## " + iter + " completed. " + threshold
            + " preStress " + preStress + " diffStress " + diffStress
            + " stress " + stress);
        }
        if (iter % 100 == 0) {
          logMemUsage();
          logGCTime();
        }
        if (iter >= MDSConstants.MAX_ITER) {
          break;
        }
        preStress = stress;
      }
      LOG.info("ITERATION ## " + iter + " completed. " + " diffStress "
        + diffStress + " stress " + stress);
      context.progress();
      tCur *= alpha;
      iter = 0;
    }
    // T == 0
    tCur = 0.0;
    iter = 0;
    preStress = calculateStress(rowDataList, xPartitions, tCur,
      sumOrigDistanceSquare, pool);
    diffStress = threshold + 1.0;
    LOG.info("%%%%%%%%%%%%%%%%%%%%%%");
    LOG.info("% T_Cur = " + tCur);
    LOG.info("%%%%%%%%%%%%%%%%%%%%%%");
    while (diffStress > threshold) {
      List<XRowData> bcList = calculateBC(rowDataList, xPartitions,
        tCur, pool);
      // LOG.info("BC end");
      // Temporarily no release but forward to cg iterations
      // Release old x partitions
      // releasePartitions(xPartitions);
      // xTable = null;
      // xPartitions = null;
      
      // xTable = conjugateGradient(rowDataList, bcList, pool);
      xTable = conjugateGradient(rowDataList, bcList, xTable, pool);
      // LOG.info("CG end");
      xPartitions = xTable.getPartitions();
      // Release bc
      for (XRowData bc : bcList) {
        pool.getDoubleArrayPool().releaseArrayInUse(bc.array.getArray());
      }
      bcList = null;
      stress = calculateStress(rowDataList, xPartitions, tCur,
        sumOrigDistanceSquare, pool);
      // LOG.info("Stress end");
      diffStress = preStress - stress;
      iter++;
      smacofRealIter++;
      if (iter % 10 == 0) {
        LOG.info("Iteration ## " + iter + " completed. " + threshold + " "
          + diffStress + " " + stress);
        context.progress();
      }
      if (iter % 100 == 0) {
        logMemUsage();
        logGCTime();
      }
      // Probably I need to break here
      if (iter >= MDSConstants.MAX_ITER) {
        break;
      }
      preStress = stress;
    }
    LOG.info("ITERATION ## " + iter + " completed. " + threshold + " "
      + diffStress + " " + stress);
    qoR1 = stress / (n * (n - 1) / 2);
    qoR2 = qoR1 / (avgOrigDistance * avgOrigDistance);
    LOG.info("Normalize1 = " + qoR1 + " Normalize2 = " + qoR2);
    LOG.info("Average of Delta(original distance) = " + avgOrigDistance);
    endTime = System.currentTimeMillis();
    double finalStress = calculateStress(rowDataList, xPartitions, tCur,
      sumOrigDistanceSquare, pool);
    // Store X file
    if (labelsFile.endsWith("NoLabel")) {
      XFileUtil.storeXOnMaster(conf, xTable, d, xOutFile, this.isMaster());
    } else {
      XFileUtil.storeXOnMaster(conf, xTable, d, xOutFile, this.isMaster(),
        labelsFile);
    }
    releasePartitions(xPartitions);
    xTable = null;
    xPartitions = null;
    LOG.info("===================================================");
    LOG.info("CG REAL ITER:" + cgRealIter);
    LOG.info("SMACOF REAL ITER: " + smacofRealIter);
    LOG.info("Init X Time " + totalInitXTime);
    LOG.info("Init X  Count " + totalInitXCount);
    LOG.info("Stress Time " + totalStressTime);
    LOG.info("Stress Count " + totalStressCount);
    LOG.info("BC Time " + totalBCTime);
    LOG.info("BC Count " + totalBCCount);
    LOG.info("MM Time " + totalMMTime);
    LOG.info("MM Count " + totalMMCount);
    LOG.info("Inner Product Time " + totalInnerProductTime);
    LOG.info("Inner Product Count " + totalInnerProductCount);
    LOG.info("Allgather Time " + totalAllgatherTime);
    LOG.info("Allgather Sync Time " + totalAllgatherSyncTime);
    LOG.info("Allgather Count " + totalAllgatherCount);
    LOG.info("Allgather Val Time " + totalAllgatherValTime);
    LOG.info("Allgather Val Sync Time " + totalAllgatherValSyncTime);
    LOG.info("Allgather Val Count " + totalAllgatherValCount);
    LOG.info("For CG iter: " + ((double) cgRealIter / (double) smacofRealIter)
      + "\tFinal Result is:\t" + finalStress + "\t" + (endTime - startTime)
      / 1000 + " seconds.");
    LOG.info("===================================================");
  }

  private List<RowData> loadData(String partitionFile, Context context)
    throws IOException {
    // Read partition file
    LOG.info("Reading the partition file: " + partitionFile);
    List<RowData> rowDataList = new ArrayList<RowData>();
    totalPartitions = DataFileUtil.readPartitionFile(partitionFile, idsFile,
      conf, rowDataList);
    LOG.info("Total partitions: " + totalPartitions);
    for (RowData rowData : rowDataList) {
      LOG.info(rowData.height + " " + rowData.width + " " + rowData.row + " "
        + rowData.rowOffset);
      LOG.info(rowData.distPath);
      LOG.info(rowData.weightPath);
      LOG.info(rowData.vPath);
    }
    // Load data
    doTasks(rowDataList, "data-load-task", new DataLoadTask(context),
      maxNumThreads);
    return rowDataList;
  }

  private ArrTable<DoubleArray, DoubleArrPlus> initializeX(int d,
    List<RowData> rowDataList, ResourcePool pool) throws Exception {
    ArrTable<DoubleArray, DoubleArrPlus> xTable = new ArrTable<DoubleArray, DoubleArrPlus>(
      getNextTableID(), DoubleArray.class, DoubleArrPlus.class);
    long startTime = System.currentTimeMillis();
    List<ArrPartition<DoubleArray>> xParList = doTasks(rowDataList,
      "x-initialize-task", new XInitializeTask(d, pool), maxNumThreads);
    long endTime = System.currentTimeMillis();
    totalInitXTime += (endTime - startTime);
    totalInitXCount += 1;
    for (ArrPartition<DoubleArray> xPar : xParList) {
      xTable.addPartition(xPar);
    }
    // startTime = System.nanoTime();
    // masterBarrier();
    // endTime = System.nanoTime();
    // totalAllgatherSyncTime += (endTime - startTime);
    startTime = System.nanoTime();
    allgatherTotalKnown(xTable, totalPartitions);
    endTime = System.nanoTime();
    totalAllgatherTime += (endTime - startTime);
    totalAllgatherCount += 1;
    return xTable;
  }

  private double[] calculateAvgDistances(List<RowData> rowDataList,
    ResourcePool pool) throws Exception {
    List<DoubleArray> avgDistList = doTasks(rowDataList, "avg-dist-calc-task",
      new AvgDistCalcTask(pool), maxNumThreads);
    // avgDist is all-gathered
    DoubleArray avgDist = allgatherVal(avgDistList, AvgDistArrCombiner.class,
      pool);
    return avgDist.getArray();
  }

  private double calculateStress(List<RowData> rowDataList,
    ArrPartition<DoubleArray>[] xPartitions, double tCur,
    double sumOrigDistanceSquare, ResourcePool pool) throws Exception {
    long startTime = System.currentTimeMillis();
    List<DoubleArray> stressList = doTasks(rowDataList, "calculate-stress",
      new StressCalcTask(xPartitions, d, tCur, pool), maxNumThreads);
    long endTime = System.currentTimeMillis();
    totalStressTime += (endTime - startTime);
    totalStressCount += 1;
    // avgDist is all-gathered
    DoubleArray stress = allgatherVal(stressList, DoubleArrPlus.class, pool);
    double stressVal = stress.getArray()[0] / sumOrigDistanceSquare;
    pool.getDoubleArrayPool().releaseArrayInUse(stress.getArray());
    return stressVal;
  }

  private <CD extends ArrCombiner<DoubleArray>> DoubleArray allgatherVal(
    List<DoubleArray> values, Class<CD> combinerClass, ResourcePool pool)
    throws Exception {
    // Create table
    ArrTable<DoubleArray, CD> arrTable = new ArrTable<DoubleArray, CD>(
      getNextTableID(), DoubleArray.class, combinerClass);
    // Create DoubleArray
    for (DoubleArray value : values) {
      // Create partition
      // Use uniformed partition id for combining
      ArrPartition<DoubleArray> arrPartition = new ArrPartition<DoubleArray>(
        value, 0);
      // Insert array to partition
      try {
        if (arrTable.addPartition(arrPartition)) {
          pool.getDoubleArrayPool().releaseArrayInUse(value.getArray());
        }
      } catch (Exception e) {
        LOG.error("Fail to add partition to table", e);
      }
    }
    // Array in AggDblVal is small, do allgather directly
    // not allreduce based on partitions.
    // After allgather, data received from other worker should be released
    // long startTime = System.nanoTime();
    // masterBarrier();
    // long endTime = System.nanoTime();
    // totalAllgatherValSyncTime += (endTime - startTime);
    long startTime = System.nanoTime();
    allgatherOne(arrTable);
    long endTime = System.nanoTime();
    totalAllgatherValTime += (endTime - startTime);
    totalAllgatherValCount += 1;
    DoubleArray finalVal = arrTable.getPartitions()[0].getArray();
    return finalVal;
  }

  private List<XRowData> calculateBC(List<RowData> rowDataList,
    ArrPartition<DoubleArray>[] xPartitions, double tCur,
    ResourcePool pool) {
    // Get new X table
    long startTime = System.currentTimeMillis();
    List<XRowData> bcList = doTasks(rowDataList, "calculate-bc",
      new BCCalcTask(xPartitions, d, tCur, pool), maxNumThreads);
    long endTime = System.currentTimeMillis();
    totalBCTime += (endTime - startTime);
    totalBCCount += 1;
    return bcList;
  }

  private ArrTable<DoubleArray, DoubleArrPlus> conjugateGradient(
    List<RowData> rowDataList, List<XRowData> pList,  ArrTable<DoubleArray, DoubleArrPlus> xTable, ResourcePool pool)
    throws Exception {
    // pList is also called bcList
    // Generate X in distributed memory and allgather them
    // ArrTable<DoubleArray, DoubleArrPlus> xTable = initializeX(d, rowDataList,
    //  this.getResourcePool());
    ArrPartition<DoubleArray>[] xPartitions = xTable.getPartitions();
    if (xPartitions.length != totalPartitions) {
      throw new Exception("Fail to initialize X.");
    }
    // Multiply X with V data, get r
    List<XRowData> rList = calculateMM(rowDataList, xPartitions, pool);
    // LOG.info("mm rList");
    // r is distributed, create rMap
    Map<Integer, XRowData> rMap = createXMapFromXRowList(rList);
    Map<Integer, XRowData> pMap = createXMapFromXRowList(pList);
    // Update p and r
    // r and p both should be the same as x in size and partitioning
    double[] ps;
    double[] rs;
    for (XRowData p : pList) {
      ps = p.array.getArray();
      rs = rMap.get(p.row).array.getArray();
      for (int i = 0; i < p.array.getSize(); i++) {
        ps[i] = ps[i] - rs[i];
        rs[i] = ps[i];
      }
    }
    // r is distributed
    // calculate rTr needs allgather partial rTr
    double rTr = calculateInnerProduct(rList, null, pool);
    // LOG.info("rTr " + rTr);
    int cgCount = 0;
    double[] aps;
    while (cgCount < cgIter) {
      // p is distributed, to do matrix multiplication
      // we need to allgather p.
      // p should be partitioned the same as x.
      // Note that pList, pMap holds the local partitions.
      // pTable holds all the partitions.
      ArrTable<DoubleArray, DoubleArrPlus> pTable = createXTableFromXRowList(pList);
      // long startTime = System.nanoTime();
      // masterBarrier();
      // long endTime = System.nanoTime();
      // totalAllgatherSyncTime += (endTime - startTime);
      long startTime = System.nanoTime();
      allgatherTotalKnown(pTable, totalPartitions);
      long endTime = System.nanoTime();
      totalAllgatherTime += (endTime - startTime);
      totalAllgatherCount += 1;
      ArrPartition<DoubleArray>[] pPartitions = pTable.getPartitions();
      List<XRowData> apList = calculateMM(rowDataList, pPartitions, pool);
      // LOG.info("mm apList");
      // Calculate alpha
      // ap is distributed, we use ap to find related p (based on row ID)
      double ip = calculateInnerProduct(apList, pMap, pool);
      // LOG.info("ip for alpha " + ip);
      double alpha = rTr / ip;
      // LOG.info("alpha " + alpha);
      // Now X is duplicated on all workers
      // do normalizedValue directly
      double sum1 = calculateNormalizedVal(xPartitions);
      // update X_i to X_i+1
      // p is duplicated on all the workers as x
      // update X directly
      updateX(xPartitions, pPartitions, alpha);
      double sum2 = calculateNormalizedVal(xPartitions);
      // LOG.info("sum1 " + sum1);
      // LOG.info("sum2 " + sum2);
      // Temporarily disable this
      if (Math.abs(sum2 - sum1) < 10E-3) {
        // The final iteration
        // Release before exiting from the loop
        // Release ap
        for (XRowData ap : apList) {
          pool.getDoubleArrayPool().releaseArrayInUse(ap.array.getArray());
        }
        apList = null;
        // Release p in pTable but not in pList
        // LOG.info("pMap size " + pMap.size() + " pPartitions size "
        // + pPartitions.length);
        for (ArrPartition<DoubleArray> pPartition : pPartitions) {
          if (!pMap.containsKey(pPartition.getPartitionID())) {
            pool.getDoubleArrayPool().releaseArrayInUse(
              pPartition.getArray().getArray());
          }
        }
        pTable = null;
        pPartitions = null;
        cgCount++;
        cgRealIter++;
        break;
      }
      // update r_i to r_i+1
      // r is distributed, ap is distributed
      // but the distribution of r should match with the distribution of ap
      // (both are based on V's distribution)
      for (XRowData ap : apList) {
        aps = ap.array.getArray();
        rs = rMap.get(ap.row).array.getArray();
        for (int i = 0; i < ap.array.getSize(); i++) {
          rs[i] = rs[i] - alpha * aps[i];
        }
      }
      // Calculate beta
      double rTr1 = calculateInnerProduct(rList, null, pool);
      // LOG.info("rTr1 " + rTr1);
      double beta = rTr1 / rTr;
      // LOG.info("beta " + beta);
      rTr = rTr1;
      // Update p_i to p_i+1
      // We only update p in pList, which are the local partitions
      for (XRowData p : pList) {
        ps = p.array.getArray();
        rs = rMap.get(p.row).array.getArray();
        for (int i = 0; i < p.array.getSize(); i++) {
          ps[i] = rs[i] + beta * ps[i];
        }
      }
      // Release ap
      for (XRowData ap : apList) {
        pool.getDoubleArrayPool().releaseArrayInUse(ap.array.getArray());
      }
      apList = null;
      // Release p in pTable but not in pList
      // LOG.info("pMap size " + pMap.size() + " pPartitions size "
      // + pPartitions.length);
      for (ArrPartition<DoubleArray> pPartition : pPartitions) {
        if (!pMap.containsKey(pPartition.getPartitionID())) {
          pool.getDoubleArrayPool().releaseArrayInUse(
            pPartition.getArray().getArray());
        }
      }
      pTable = null;
      pPartitions = null;
      cgCount++;
      cgRealIter++;
    }
    // Release rList, rMap
    for (XRowData r : rList) {
      pool.getDoubleArrayPool().releaseArrayInUse(r.array.getArray());
    }
    rList = null;
    rMap = null;
    // Release pList, pMap
    // Because pList is a parameter, arrays are released outside
    pList = null;
    pMap = null;
    return xTable;
  }

  private Map<Integer, XRowData> createXMapFromXRowList(
    List<XRowData> xRowDataList) {
    Map<Integer, XRowData> xMap = new HashMap<Integer, XRowData>();
    for (XRowData x : xRowDataList) {
      xMap.put(x.row, x);
    }
    return xMap;
  }

  private List<XRowData> calculateMM(List<RowData> rowDataList,
    ArrPartition<DoubleArray>[] xPartitions, ResourcePool pool) {
    // Do matrix multiplication
    // The result is same as X in size
    long startTime = System.currentTimeMillis();
    List<XRowData> xRowDataList = doTasks(rowDataList, "calculate-mm",
      new MMCalcTask(xPartitions, d, pool), maxNumThreads);
    long endTime = System.currentTimeMillis();
    totalMMTime += (endTime - startTime);
    totalMMCount += 1;
    return xRowDataList;
  }

  private double calculateInnerProduct(List<XRowData> xRowDataList,
    Map<Integer, XRowData> refMap, ResourcePool pool) throws Exception {
    long startTime = System.currentTimeMillis();
    List<DoubleArray> values = doTasks(xRowDataList, "calculate-innerproduct",
      new InnerProductCalcTask(refMap, pool), maxNumThreads);
    long endTime = System.currentTimeMillis();
    totalInnerProductTime += (endTime - startTime);
    totalInnerProductCount += 1;
    // avgDist is all-gathered
    DoubleArray product = allgatherVal(values, DoubleArrPlus.class, pool);
    double productVal = product.getArray()[0];
    pool.getDoubleArrayPool().releaseArrayInUse(product.getArray());
    return productVal;
  }

  private ArrTable<DoubleArray, DoubleArrPlus> createXTableFromXRowList(
    List<XRowData> xRowDataList) throws Exception {
    ArrTable<DoubleArray, DoubleArrPlus> xTable = new ArrTable<DoubleArray, DoubleArrPlus>(
      getNextTableID(), DoubleArray.class, DoubleArrPlus.class);
    for (XRowData xRowData : xRowDataList) {
      xTable.addPartition(new ArrPartition<DoubleArray>(xRowData.array,
        xRowData.row));
    }
    return xTable;
  }

  private static double calculateNormalizedVal(
    ArrPartition<DoubleArray>[] xPartitions) {
    double sum = 0;
    DoubleArray array;
    double[] doubles;
    for (ArrPartition<DoubleArray> xPartition : xPartitions) {
      array = xPartition.getArray();
      doubles = array.getArray();
      for (int i = 0; i < array.getSize(); i++)
        sum += doubles[i] * doubles[i];
    }
    return Math.sqrt(sum);
  }

  private void updateX(ArrPartition<DoubleArray>[] xPartitions,
    ArrPartition<DoubleArray>[] pPartitions, double alpha) {
    DoubleArray xArray;
    double[] xDoubles;
    DoubleArray pArray;
    double[] pDoubles;
    for (int i = 0; i < xPartitions.length; i++) {
      xArray = xPartitions[i].getArray();
      xDoubles = xArray.getArray();
      pArray = pPartitions[i].getArray();
      pDoubles = pArray.getArray();
      for (int j = 0; j < xArray.getSize(); j++)
        xDoubles[j] += alpha * pDoubles[j];
    }
  }

  private void releasePartitions(ArrPartition<DoubleArray>[] partitions) {
    for (int i = 0; i < partitions.length; i++) {
      this.getResourcePool().getDoubleArrayPool()
        .releaseArrayInUse(partitions[i].getArray().getArray());
    }
  }

  private int getNextTableID() {
    return tableID++;
  }
}
