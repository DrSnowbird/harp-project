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

package edu.iu.harp.collective;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.WorkerInfo;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.ByteArrReqSender;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.request.MultiBinAllToAllReq;
import edu.iu.harp.comm.request.MultiBinParGenAck;
import edu.iu.harp.comm.request.MultiBinRegroupReq;
import edu.iu.harp.comm.request.ParGenAck;
import edu.iu.harp.comm.request.ReqAck;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.Receiver;
import edu.iu.harp.graph.EdgePartition;
import edu.iu.harp.graph.EdgeTable;
import edu.iu.harp.graph.EdgeVal;
import edu.iu.harp.graph.InEdgeTable;
import edu.iu.harp.graph.IntMsgVal;
import edu.iu.harp.graph.LongVertexID;
import edu.iu.harp.graph.MsgPartition;
import edu.iu.harp.graph.MsgTable;
import edu.iu.harp.graph.MsgVal;
import edu.iu.harp.graph.NullEdgeVal;
import edu.iu.harp.graph.VertexID;
import edu.iu.harp.graph.vtx.LongDblVtxPartition;
import edu.iu.harp.graph.vtx.LongDblVtxTable;
import edu.iu.harp.graph.vtx.LongIntVtxPartition;
import edu.iu.harp.graph.vtx.LongIntVtxTable;
import edu.iu.harp.graph.vtx.MultiStructParCatcher;
import edu.iu.harp.graph.vtx.StructPartition;
import edu.iu.harp.graph.vtx.StructTable;

public class GraphWorker extends CollCommWorker {

  public static void main(String args[]) throws Exception {
    String driverHost = args[0];
    int driverPort = Integer.parseInt(args[1]);
    int workerID = Integer.parseInt(args[2]);
    long jobID = Long.parseLong(args[3]);
    int iteration = Integer.parseInt(args[4]);
    initLogger(workerID);
    LOG.info("args[] " + driverHost + " " + driverPort + " " + workerID + " "
      + jobID + " " + iteration);
    // --------------------------------------------------------------------
    // Worker initialize
    Workers workers = new Workers(workerID);
    String host = workers.getSelfInfo().getNode();
    int port = workers.getSelfInfo().getPort();
    WorkerData workerData = new WorkerData();
    ResourcePool resourcePool = new ResourcePool();
    Receiver receiver = new Receiver(workerData, resourcePool, workers, host,
      port, Constants.NUM_HANDLER_THREADS);
    receiver.start();
    // Master check if all slaves are ready
    boolean success = masterHandshake(workers, workerData, resourcePool);
    LOG.info("Barrier: " + success);
    // ------------------------------------------------------------------------
    // Generate in-edge table and vertex table (for pagerank value) , (for out
    // edge count)
    int numParPerWorker = 2;
    int vtxCountPerWorker = 5;
    int edgeCountPerWorker = 3;
    int numWorkers = workers.getNumWorkers();
    int totalVertexCount = vtxCountPerWorker * numWorkers;
    LOG.info("Total vtx count: " + totalVertexCount);
    int totalPartitions = numParPerWorker * numWorkers;
    InEdgeTable<LongVertexID, NullEdgeVal> inEdgeTable = new InEdgeTable<LongVertexID, NullEdgeVal>(
      0, totalPartitions, 1000, LongVertexID.class, NullEdgeVal.class,
      resourcePool);
    long source;
    long target;
    Random random = new Random(workerID);
    for (int i = 0; i < edgeCountPerWorker; i++) {
      target = random.nextInt(totalVertexCount);
      do {
        source = random.nextInt(totalVertexCount);
      } while (source == target);
      LOG.info("Edge: " + source + "->" + target);
      inEdgeTable.addEdge(new LongVertexID(source), new NullEdgeVal(),
        new LongVertexID(target));
    }
    try {
      LOG.info("PRINT EDGE TABLE START");
      printEdgeTable(inEdgeTable);
      LOG.info("PRINT EDGE TABLE END");
      LOG.info("REGROUP START");
      regroupEdges(inEdgeTable, workers, workerData, resourcePool);
      LOG.info("REGROUP End");
      LOG.info("PRINT EDGE TABLE START");
      printEdgeTable(inEdgeTable);
      LOG.info("PRINT EDGE TABLE END");
    } catch (Exception e) {
      LOG.error("Error when adding edges", e);
    }
    LongDblVtxTable ldVtxTable = new LongDblVtxTable(1, totalPartitions, 10);
    LongVertexID targetID = null;
    for (EdgePartition<LongVertexID, NullEdgeVal> partition : inEdgeTable
      .getPartitions()) {
      while (partition.nextEdge()) {
        targetID = partition.getCurTargetID();
        ldVtxTable.putVertexVal(targetID.getVertexID(),
          1.0 / (double) totalVertexCount);
      }
      partition.defaultReadPos();
    }
    printVtxTable(ldVtxTable);
    // Generate message for out edge count
    MsgTable<LongVertexID, IntMsgVal> msgTable = new MsgTable<LongVertexID, IntMsgVal>(
      2, totalPartitions, 12, LongVertexID.class, IntMsgVal.class, resourcePool);

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
    // all-to-all communication, moves message partition to the place
    // where the vertex partition locate
    LOG.info("All MSG TO ALL VTX START");
    try {
      allMsgToAllVtx(msgTable, ldVtxTable, workers, workerData, resourcePool);
    } catch (Exception e) {
      LOG.error("Error in all msg to all vtx", e);
    }
    LOG.info("All MSG TO ALL VTX END");
    // Another vertex table for out edge count
    LongIntVtxTable liVtxTable = new LongIntVtxTable(3, totalPartitions, 10);
    // Process msg table
    try {
      MsgPartition<LongVertexID, IntMsgVal>[] msgPartitions = msgTable
        .getPartitions();
      for (MsgPartition<LongVertexID, IntMsgVal> partition : msgPartitions) {
        while (partition.nextMsg()) {
          liVtxTable.addVertexVal(partition.getCurVertexID().getVertexID(),
            partition.getCurMsgVal().getIntMsgVal());
          LOG.info("MSG: " + partition.getCurVertexID().getVertexID() + " "
            + partition.getCurMsgVal().getIntMsgVal());
        }
      }
    } catch (Exception e) {
      LOG.error("Error when processing msg.", e);
    }
    LOG.info("PRINT EDGE COUNT.");
    printVtxTable(liVtxTable);
    // -----------------------------------------------------------------------------
    reportWorkerStatus(resourcePool, workerID, driverHost, driverPort);
    receiver.stop();
    System.exit(0);
  }

  private static void printVtxTable(LongDblVtxTable ldVtxTable) {
    LongDblVtxPartition[] vtxPartitions = ldVtxTable.getPartitions();
    for (LongDblVtxPartition partition : vtxPartitions) {
      for (Long2DoubleMap.Entry entry : partition.getVertexMap()
        .long2DoubleEntrySet()) {
        LOG.info("Patition: " + partition.getPartitionID() + " "
          + entry.getLongKey() + " " + entry.getDoubleValue());
      }
    }
  }

  private static void printVtxTable(LongIntVtxTable liVtxTable) {
    LongIntVtxPartition[] vtxPartitions = liVtxTable.getPartitions();
    for (LongIntVtxPartition partition : vtxPartitions) {
      for (Long2IntMap.Entry entry : partition.getVertexMap()
        .long2IntEntrySet()) {
        LOG.info(entry.getLongKey() + " " + entry.getIntValue());
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

  public static <P extends StructPartition, T extends StructTable<P>, I extends VertexID, M extends MsgVal> void allMsgToAllVtx(
    MsgTable<I, M> msgTable, T vtxTable, Workers workers,
    WorkerData workerData, ResourcePool resourcePool) throws Exception {
    if (workers.getNumWorkers() == 1) {
      return;
    }
    int workerID = workers.getSelfID();
    int numWorkers = workers.getNumWorkers();
    // -------------------------------------------------------------------------
    // Gather the information of generated vertex partitions to master
    int[] vtxPIDs = vtxTable.getPartitionIDs();
    ParGenAck vtxPGenAck = new ParGenAck(workerID, vtxPIDs);
    ParGenAck[][] vtxPGenAckRef = new ParGenAck[1][];
    LOG.info("Gather vtx partition info.");
    try {
      reqGather(workers, workerData, vtxPGenAck, vtxPGenAckRef, resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering msg partition info.", e);
      return;
    }
    LOG.info("All vtx partition info are gathered.");
    // ---------------------------------------------------------------------------
    // Gather the information of generated msg partitions to master
    Int2IntOpenHashMap msgPartitionCount = new Int2IntOpenHashMap(
      msgTable.getNumPartitions());
    for (MsgPartition<I, M> partition : msgTable.getPartitions()) {
      msgPartitionCount.put(partition.getPartitionID(), partition.getData()
        .size());
    }
    MultiBinParGenAck msgPGenAck = new MultiBinParGenAck(workerID,
      msgPartitionCount);
    MultiBinParGenAck[][] msgPGenAckRef = new MultiBinParGenAck[1][];
    LOG.info("Gather msg partition info.");
    try {
      reqGather(workers, workerData, msgPGenAck, msgPGenAckRef, resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering msg partition info.", e);
      return;
    }
    LOG.info("All msg partition info are gathered.");
    // --------------------------------------------------------------------------
    // Generate partition and worker mapping for regrouping
    // Bcast partition regroup request
    MultiBinAllToAllReq msgRegroupReq = null;
    if (workers.isMaster()) {
      ParGenAck[] vtxPGenAcks = vtxPGenAckRef[0];
      MultiBinParGenAck[] msgPGenAcks = msgPGenAckRef[0];
      // partition <-> workers mapping
      // based on vertex partition info
      Int2ObjectOpenHashMap<IntArrayList> partitionWorkers = new Int2ObjectOpenHashMap<IntArrayList>();
      for (int i = 0; i < vtxPGenAcks.length; i++) {
        int[] parIds = vtxPGenAcks[i].getPartitionIds();
        for (int j = 0; j < parIds.length; j++) {
          IntArrayList list = partitionWorkers.get(parIds[j]);
          if (list == null) {
            list = new IntArrayList();
            partitionWorkers.put(parIds[j], list);
          }
          list.add(vtxPGenAcks[i].getWorkerID());
        }
        // Free vtxPGenAck, no use in future
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(
          vtxPGenAcks[i]);
      }
      // worker <-> data count
      // based on msg partition info
      Int2IntOpenHashMap workerPartitionCount = new Int2IntOpenHashMap();
      IntArrayList destWorkerIDs;
      int hashDestID;
      for (int i = 0; i < msgPGenAcks.length; i++) {
        Int2IntOpenHashMap dataCount = msgPGenAcks[i].getParDataCount();
        for (Int2IntMap.Entry entry : dataCount.int2IntEntrySet()) {
          destWorkerIDs = partitionWorkers.get(entry.getIntKey());
          // There could be no destination for this msg partition
          if (destWorkerIDs != null) {
            for (int destID : destWorkerIDs) {
              workerPartitionCount.addTo(destID, entry.getIntValue());
            }
          } else {
            hashDestID = entry.getIntKey() % numWorkers;
            // Because this msg partition cannot find worker destination
            // with matched vertex partition ID, we hash the partition ID to
            // a worker ID, and add it to partition <-> worker mapping
            IntArrayList list = partitionWorkers.get(entry.getIntKey());
            if (list == null) {
              list = new IntArrayList();
              partitionWorkers.put(entry.getIntKey(), list);
            }
            list.add(hashDestID);
            workerPartitionCount.addTo(hashDestID, entry.getIntValue());
            LOG
              .info("MSG Partition "
                + entry.getKey()
                + "doesn't know target vertex partition location. Send to Worker "
                + hashDestID);
          }
        }
        // Free pGenAck, no use in future
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(
          msgPGenAcks[i]);
      }
      // Print partition distribution
      LOG.info("Partition : Worker");
      StringBuffer buffer = new StringBuffer();
      for (Entry<Integer, IntArrayList> entry : partitionWorkers.entrySet()) {
        for (int val : entry.getValue()) {
          buffer.append(val + " ");
        }
        LOG.info(entry.getKey() + ":" + buffer);
        buffer.delete(0, buffer.length());
      }
      LOG.info("Worker : Data Count");
      for (Entry<Integer, Integer> entry : workerPartitionCount.entrySet()) {
        LOG.info(entry.getKey() + " " + entry.getValue());
      }
      msgRegroupReq = new MultiBinAllToAllReq(partitionWorkers,
        workerPartitionCount);
    }
    // ------------------------------------------------------------------------
    LOG.info("Bcast regroup information.");
    MultiBinAllToAllReq[] msgRegroupReqRef = new MultiBinAllToAllReq[1];
    msgRegroupReqRef[0] = msgRegroupReq;
    boolean success = reqChainBcast(msgRegroupReqRef, workers, workerData,
      resourcePool, MultiBinAllToAllReq.class);
    if (!success) {
      return;
    }
    LOG.info("Regroup information is bcasted.");
    msgRegroupReq = msgRegroupReqRef[0];
    // ------------------------------------------------------------------------
    // Send partition
    int tableID = msgTable.getTableID();
    MsgPartition<I, M>[] partitions = msgTable.getPartitions();
    Int2ObjectOpenHashMap<IntArrayList> partitionWorkers = msgRegroupReq
      .getPartitionWorkerMap();
    ObjectArrayList<MsgPartition<I, M>> remvPartitions = new ObjectArrayList<MsgPartition<I, M>>();
    int localDataCount = 0;
    IntArrayList destIDs;
    boolean isSentToLocal = false;
    int[] order = createRandomNumersInRange(System.nanoTime() / 1000 * 1000
      + workerID, partitions.length);
    for (int i = 0; i < order.length; i++) {
      MsgPartition<I, M> partition = partitions[order[i]];
      destIDs = partitionWorkers.get(partition.getPartitionID());
      isSentToLocal = false;
      for (int destID : destIDs) {
        if (destID == workerID) {
          localDataCount = localDataCount + partition.getData().size();
          isSentToLocal = true;
        } else {
          WorkerInfo workerInfo = workers.getWorkerInfo(destID);
          send(tableID, partition, workerInfo.getNode(), workerInfo.getPort(),
            workerID, resourcePool);
        }
      }
      if (!isSentToLocal) {
        remvPartitions.add(partition);
      }
    }
    // ------------------------------------------------------------------------
    // Receive all the data from the queue
    Int2IntOpenHashMap workerPartitionCount = msgRegroupReq
      .getWorkerPartitionCountMap();
    int totalDataRecv = workerPartitionCount.get(workerID) - localDataCount;
    LOG.info("Total receive: " + totalDataRecv);
    Int2ObjectOpenHashMap<ObjectArrayList<ByteArray>> recvBinPartitions = new Int2ObjectOpenHashMap<ObjectArrayList<ByteArray>>();
    waitAndGetBinPartitions(recvBinPartitions, totalDataRecv, workerData,
      resourcePool, Constants.DATA_MAX_WAIT_TIME);
    // Remove partitions sent out
    for (MsgPartition<I, M> partition : remvPartitions) {
      msgTable.removePartition(partition.getPartitionID());
      partition.release();
    }
    // Add bin partitions to msg table
    for (Int2ObjectMap.Entry<ObjectArrayList<ByteArray>> entry : recvBinPartitions
      .int2ObjectEntrySet()) {
      msgTable.addAllData(entry.getIntKey(), entry.getValue());
    }
    // Free regroupReq, no use in future
    resourcePool.getWritableObjectPool().freeWritableObjectInUse(msgRegroupReq);
    LOG.info("Start collect regroup finishing information.");
    ReqAck reqAck = new ReqAck(workerID, 0);
    ReqAck[][] reqAckRef = new ReqAck[1][];
    try {
      reqCollect(workers, workerData, reqAck, reqAckRef, resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering data.", e);
      return;
    }
    // Free all rhe reqAck
    ReqAck[] reqAcks = reqAckRef[0];
    if (reqAcks != null) {
      for (ReqAck ack : reqAcks) {
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(ack);
      }
    }
    LOG.info("End collect regroup finishing information.");
  }

  public static <I extends VertexID, E extends EdgeVal, ET extends EdgeTable<I, E>, VP extends StructPartition, VT extends StructTable<VP>> void allEdgeToAllVtx(
    ET edgeTable, VT vtxTable, Workers workers, WorkerData workerData,
    ResourcePool resourcePool) throws Exception {
    if (workers.getNumWorkers() == 1) {
      return;
    }
    int workerID = workers.getSelfID();
    int numWorkers = workers.getNumWorkers();
    // -------------------------------------------------------------------------
    // Gather information of vertex partitions to master
    int[] vtxPIDs = vtxTable.getPartitionIDs();
    ParGenAck vtxPGenAck = new ParGenAck(workerID, vtxPIDs);
    ParGenAck[][] vtxPGenAckRef = new ParGenAck[1][];
    LOG.info("Gather VTX info.");
    try {
      reqGather(workers, workerData, vtxPGenAck, vtxPGenAckRef, resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering msg partition info.", e);
      return;
    }
    LOG.info("VTX info are gathered.");
    // ---------------------------------------------------------------------------
    // Gather information of edge partitions to master
    Int2IntOpenHashMap edgeParDataCount = new Int2IntOpenHashMap(
      edgeTable.getMaxNumPartitions());
    for (EdgePartition<I, E> partition : edgeTable.getPartitions()) {
      edgeParDataCount.put(partition.getPartitionID(), partition.getData()
        .size());
    }
    MultiBinParGenAck edgePGenAck = new MultiBinParGenAck(workerID,
      edgeParDataCount);
    MultiBinParGenAck[][] edgePGenAckRef = new MultiBinParGenAck[1][];
    LOG.info("Gather edge partition info.");
    try {
      reqGather(workers, workerData, edgePGenAck, edgePGenAckRef, resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering edge partition info.", e);
      return;
    }
    LOG.info("All msg partition info are gathered.");
    // --------------------------------------------------------------------------
    // Generate partition and worker mapping for regrouping
    // Bcast partition regroup request
    MultiBinAllToAllReq edgeRegroupReq = null;
    if (workers.isMaster()) {
      ParGenAck[] vtxPGenAcks = vtxPGenAckRef[0];
      MultiBinParGenAck[] edgePGenAcks = edgePGenAckRef[0];
      // Partition <-> Workers mapping (from vtx partition)
      Int2ObjectOpenHashMap<IntArrayList> partitionWorkers = new Int2ObjectOpenHashMap<IntArrayList>();
      for (int i = 0; i < vtxPGenAcks.length; i++) {
        int[] parIds = vtxPGenAcks[i].getPartitionIds();
        for (int j = 0; j < parIds.length; j++) {
          IntArrayList list = partitionWorkers.get(parIds[j]);
          if (list == null) {
            list = new IntArrayList();
            partitionWorkers.put(parIds[j], list);
          }
          list.add(vtxPGenAcks[i].getWorkerID());
        }
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(
          vtxPGenAcks[i]);
      }
      // Worker <-> Data Count (from edge partition)
      Int2IntOpenHashMap workerPartitionCount = new Int2IntOpenHashMap();
      workerPartitionCount.defaultReturnValue(0);
      IntArrayList destWorkerIDs;
      int hashDestID;
      for (int i = 0; i < edgePGenAcks.length; i++) {
        Int2IntOpenHashMap dataCount = edgePGenAcks[i].getParDataCount();
        for (Int2IntMap.Entry entry : dataCount.int2IntEntrySet()) {
          destWorkerIDs = partitionWorkers.get(entry.getKey());
          if (destWorkerIDs != null) {
            for (int destID : destWorkerIDs) {
              workerPartitionCount.addTo(destID, entry.getValue());
            }
          } else {
            hashDestID = entry.getIntKey() % numWorkers;
            // Because this edge partition cannot find worker destination
            // with matched vertex partition ID, we hash the partition ID to
            // a worker ID, and add it to partition <-> worker mapping
            IntArrayList list = partitionWorkers.get(entry.getIntKey());
            if (list == null) {
              list = new IntArrayList();
              partitionWorkers.put(entry.getIntKey(), list);
            }
            list.add(hashDestID);
            workerPartitionCount.addTo(hashDestID, entry.getIntValue());
            LOG
              .info("Edge Partition "
                + entry.getKey()
                + "doesn't know target vertex partition location. Send to Worker "
                + hashDestID);
          }
        }
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(
          edgePGenAcks[i]);
      }
      // Print partition distribution
      LOG.info("Partition : Worker");
      StringBuffer buffer = new StringBuffer();
      for (Entry<Integer, IntArrayList> entry : partitionWorkers.entrySet()) {
        for (int val : entry.getValue()) {
          buffer.append(val + " ");
        }
        LOG.info(entry.getKey() + ":" + buffer);
        buffer.delete(0, buffer.length());
      }
      LOG.info("Worker : Data Count");
      for (Entry<Integer, Integer> entry : workerPartitionCount.entrySet()) {
        LOG.info(entry.getKey() + " " + entry.getValue());
      }
      edgeRegroupReq = new MultiBinAllToAllReq(partitionWorkers,
        workerPartitionCount);
    }
    // ------------------------------------------------------------------------
    LOG.info("Bcast regroup information.");
    MultiBinAllToAllReq[] edgeRegroupReqRef = new MultiBinAllToAllReq[1];
    edgeRegroupReqRef[0] = edgeRegroupReq;
    boolean success = reqChainBcast(edgeRegroupReqRef, workers, workerData,
      resourcePool, MultiBinAllToAllReq.class);
    if (!success) {
      return;
    }
    LOG.info("Regroup information is bcasted.");
    edgeRegroupReq = edgeRegroupReqRef[0];
    // ------------------------------------------------------------------------
    // Send partition
    int tableID = edgeTable.getTableID();
    EdgePartition<I, E>[] partitions = edgeTable.getPartitions();
    Int2ObjectOpenHashMap<IntArrayList> partitionWorkers = edgeRegroupReq
      .getPartitionWorkerMap();
    ObjectArrayList<EdgePartition<I, E>> remvPartitions = new ObjectArrayList<EdgePartition<I, E>>();
    int localDataCount = 0;
    IntArrayList destIDs;
    boolean isSentToLocal = false;
    int[] order = createRandomNumersInRange(System.nanoTime() / 1000 * 1000
      + workerID, partitions.length);
    for (int i = 0; i < order.length; i++) {
      EdgePartition<I, E> partition = partitions[order[i]];
      destIDs = partitionWorkers.get(partition.getPartitionID());
      isSentToLocal = false;
      for (int destID : destIDs) {
        if (destID == workerID) {
          localDataCount = localDataCount + partition.getData().size();
          isSentToLocal = true;
        } else {
          WorkerInfo workerInfo = workers.getWorkerInfo(destID);
          send(tableID, partition, workerInfo.getNode(), workerInfo.getPort(),
            workerID, resourcePool);
        }
      }
      if (!isSentToLocal) {
        remvPartitions.add(partition);
      }
    }
    // ------------------------------------------------------------------------
    // Receive all the data from the queue
    Int2IntOpenHashMap workerPartitionCount = edgeRegroupReq
      .getWorkerPartitionCountMap();
    int totalDataRecv = workerPartitionCount.get(workerID) - localDataCount;
    LOG.info("Total receive: " + totalDataRecv);
    Int2ObjectOpenHashMap<ObjectArrayList<ByteArray>> recvBinPartitions = new Int2ObjectOpenHashMap<ObjectArrayList<ByteArray>>();
    waitAndGetBinPartitions(recvBinPartitions, totalDataRecv, workerData, resourcePool,
      Constants.DATA_MAX_WAIT_TIME);
    // Remove partitions sent out
    for (EdgePartition<I, E> partition : remvPartitions) {
      edgeTable.removePartition(partition.getPartitionID());
      partition.release();
    }
    // Add bin partitions to edge table
    for (Int2ObjectMap.Entry<ObjectArrayList<ByteArray>> entry : recvBinPartitions
      .int2ObjectEntrySet()) {
      edgeTable.addAllData(entry.getIntKey(), entry.getValue());
    }
    // Free regroupReq, no use in future
    resourcePool.getWritableObjectPool()
      .freeWritableObjectInUse(edgeRegroupReq);
    LOG.info("Start collect regroup finishing information.");
    ReqAck reqAck = new ReqAck(workerID, 0);
    ReqAck[][] reqAckRef = new ReqAck[1][];
    try {
      reqCollect(workers, workerData, reqAck, reqAckRef, resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering data.", e);
      return;
    }
    // Free all rhe reqAck
    ReqAck[] reqAcks = reqAckRef[0];
    if (reqAcks != null) {
      for (ReqAck ack : reqAcks) {
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(ack);
      }
    }
    LOG.info("End collect regroup finishing information.");
  }

  public static <I extends VertexID, E extends EdgeVal, ET extends EdgeTable<I, E>> void regroupEdges(
    ET edgeTable, Workers workers, WorkerData workerData,
    ResourcePool resourcePool) throws Exception {
    if (workers.getNumWorkers() == 1) {
      return;
    }
    int workerID = workers.getSelfID();
    int numWorkers = workers.getNumWorkers();
    // ---------------------------------------------------------------------------
    // Gather information of edge partitions to master
    Int2IntOpenHashMap edgeParDataCount = new Int2IntOpenHashMap(
      edgeTable.getMaxNumPartitions());
    for (EdgePartition<I, E> partition : edgeTable.getPartitions()) {
      edgeParDataCount.put(partition.getPartitionID(), partition.getData()
        .size());
    }
    MultiBinParGenAck edgePGenAck = new MultiBinParGenAck(workerID,
      edgeParDataCount);
    MultiBinParGenAck[][] edgePGenAckRef = new MultiBinParGenAck[1][];
    // LOG.info("Gather edge partition info.");
    try {
      reqGather(workers, workerData, edgePGenAck, edgePGenAckRef, resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering edge partition info.", e);
      return;
    }
    // LOG.info("All msg partition info are gathered.");
    // --------------------------------------------------------------------------
    // Generate partition and worker mapping for regrouping
    // Bcast partition regroup request
    MultiBinRegroupReq edgeRegroupReq = null;
    MultiBinRegroupReq[] edgeRegroupReqRef = new MultiBinRegroupReq[1];
    if (workers.isMaster()) {
      MultiBinParGenAck[] edgePGenAcks = edgePGenAckRef[0];
      // Partition <-> Worker mapping (hash)
      Int2IntOpenHashMap partitionWorker = new Int2IntOpenHashMap();
      // Worker <-> Data Count (from edge partition)
      Int2IntOpenHashMap workerPartitionCount = new Int2IntOpenHashMap();
      workerPartitionCount.defaultReturnValue(0);
      int destWorkerID;
      for (int i = 0; i < edgePGenAcks.length; i++) {
        Int2IntOpenHashMap dataCount = edgePGenAcks[i].getParDataCount();
        for (Int2IntMap.Entry entry : dataCount.int2IntEntrySet()) {
          destWorkerID = entry.getIntKey() % numWorkers;
          partitionWorker.put(entry.getIntKey(), destWorkerID);
          workerPartitionCount.addTo(destWorkerID, entry.getValue());
        }
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(
          edgePGenAcks[i]);
      }
      // Print partition distribution
      // LOG.info("Partition : Worker");
      // for (Int2IntMap.Entry entry : partitionWorker.int2IntEntrySet()) {
      // LOG.info(entry.getIntKey() + ":" + entry.getIntValue());
      // }
      // LOG.info("Worker : Data Count");
      // for (Entry<Integer, Integer> entry : workerPartitionCount.entrySet()) {
      // LOG.info(entry.getKey() + " " + entry.getValue());
      // }
      //
      edgeRegroupReq = new MultiBinRegroupReq(partitionWorker,
        workerPartitionCount);
      edgeRegroupReqRef[0] = edgeRegroupReq;
    }
    // ------------------------------------------------------------------------
    // LOG.info("Bcast regroup information.");
    boolean success = reqChainBcast(edgeRegroupReqRef, workers, workerData,
      resourcePool, MultiBinRegroupReq.class);
    if (!success) {
      return;
    }
    // LOG.info("Regroup information is bcasted.");
    edgeRegroupReq = edgeRegroupReqRef[0];
    // ------------------------------------------------------------------------
    // Send partition
    int tableID = edgeTable.getTableID();
    EdgePartition<I, E>[] partitions = edgeTable.getPartitions();
    Int2IntOpenHashMap partitionWorker = edgeRegroupReq.getPartitionWorkerMap();
    ObjectArrayList<EdgePartition<I, E>> remvPartitions = new ObjectArrayList<EdgePartition<I, E>>();
    int localDataCount = 0;
    int destWorkerID;
    int[] order = createRandomNumersInRange(System.nanoTime() / 1000 * 1000
      + workerID, partitions.length);
    for (int i = 0; i < order.length; i++) {
      EdgePartition<I, E> partition = partitions[order[i]];
      destWorkerID = partitionWorker.get(partition.getPartitionID());
      if (destWorkerID == workerID) {
        localDataCount = localDataCount + partition.getData().size();
      } else {
        WorkerInfo workerInfo = workers.getWorkerInfo(destWorkerID);
        send(tableID, partition, workerInfo.getNode(), workerInfo.getPort(),
          workerID, resourcePool);
        remvPartitions.add(partition);
      }
    }
    // ------------------------------------------------------------------------
    // Receive all the data from the queue
    Int2IntOpenHashMap workerPartitionCount = edgeRegroupReq
      .getWorkerPartitionCountMap();
    int totalDataRecv = workerPartitionCount.get(workerID) - localDataCount;
    // LOG.info("Total receive: " + totalDataRecv);
    Int2ObjectOpenHashMap<ObjectArrayList<ByteArray>> recvBinPartitions = new Int2ObjectOpenHashMap<ObjectArrayList<ByteArray>>();
    waitAndGetBinPartitions(recvBinPartitions, totalDataRecv, workerData,
      resourcePool, Constants.DATA_MAX_WAIT_TIME);
    // Remove partitions sent out
    for (EdgePartition<I, E> partition : remvPartitions) {
      edgeTable.removePartition(partition.getPartitionID());
      partition.release();
    }
    // Add bin partitions to edge table
    for (Int2ObjectMap.Entry<ObjectArrayList<ByteArray>> entry : recvBinPartitions
      .int2ObjectEntrySet()) {
      edgeTable.addAllData(entry.getIntKey(), entry.getValue());
    }
    // Free regroupReq, no use in future
    resourcePool.getWritableObjectPool()
      .freeWritableObjectInUse(edgeRegroupReq);
    // LOG.info("Start collect regroup finishing information.");
    ReqAck reqAck = new ReqAck(workerID, 0);
    ReqAck[][] reqAckRef = new ReqAck[1][];
    try {
      reqCollect(workers, workerData, reqAck, reqAckRef, resourcePool);
    } catch (Exception e) {
      LOG.error("Error when gathering data.", e);
      return;
    }
    // Free all rhe reqAck
    ReqAck[] reqAcks = reqAckRef[0];
    if (reqAcks != null) {
      for (ReqAck ack : reqAcks) {
        resourcePool.getWritableObjectPool().freeWritableObjectInUse(ack);
      }
    }
    // LOG.info("End collect regroup finishing information.");
  }

  private static int[] createRandomNumersInRange(long seed, int max) {
    Random random = new Random(seed);
    int[] num = new int[max];
    IntOpenHashSet set = new IntOpenHashSet(max);
    int next = 0;
    for (int i = 0; i < max; i++) {
      do {
        next = random.nextInt(max);
      } while (set.contains(next));
      num[i] = next;
      set.add(next);
      // LOG.info("num_" + i + "=" + num[i]);
    }
    return num;
  }

  private static void waitAndGetBinPartitions(
    Int2ObjectOpenHashMap<ObjectArrayList<ByteArray>> recvBinPartitions,
    int totalByteArrayCount, WorkerData workerData, ResourcePool resourcePool,
    long perDataTimeOut) {
    ObjectArrayList<Commutable> skippedCommData = new ObjectArrayList<Commutable>();
    for (int i = 0; i < totalByteArrayCount; i++) {
      // Wait if data arrives
      Commutable data = workerData.waitAndGetCommData(perDataTimeOut);
      if (data == null) {
        return;
      }
      // Skip non-byte arry data.
      if (!(data instanceof ByteArray)) {
        skippedCommData.add(data);
        i--;
        continue;
      }
      ByteArray byteArray = (ByteArray) data;
      int[] metaArray = byteArray.getMetaArray();
      int partitionID = metaArray[2];
      // Once partition are got, meta array can be released
      resourcePool.getIntArrayPool().releaseArrayInUse(metaArray);
      byteArray.setMetaArray(null);
      byteArray.setMetaArraySize(0);
      ObjectArrayList<ByteArray> byteArrays = recvBinPartitions
        .get(partitionID);
      if (byteArrays == null) {
        byteArrays = new ObjectArrayList<ByteArray>();
        recvBinPartitions.put(partitionID, byteArrays);
      }
      byteArrays.add(byteArray);
    }
    if (!skippedCommData.isEmpty()) {
      workerData.putAllCommData(skippedCommData);
    }
  }

  public static <I extends VertexID, M extends MsgVal> void send(int tableID,
    MsgPartition<I, M> partition, String host, int port, int senderID,
    ResourcePool resourcePool) {
    List<ByteArray> arrays = partition.getData();
    for (int i = 0; i < arrays.size(); i++) {
      ByteArray array = arrays.get(i);
      // int[] metaData = new int[3];
      int[] metaArray = resourcePool.getIntArrayPool().getArray(3);
      metaArray[0] = senderID;
      metaArray[1] = tableID;
      metaArray[2] = partition.getPartitionID();
      array.setMetaArray(metaArray);
      array.setMetaArraySize(3);
      ByteArrReqSender sender = new ByteArrReqSender(host, port, array,
        resourcePool);
      sender.execute();
      // Once data are sent, no use for meta data
      resourcePool.getIntArrayPool().releaseArrayInUse(metaArray);
      array.setMetaArray(null);
      array.setMetaArraySize(0);
    }
  }

  public static <I extends VertexID, E extends EdgeVal> void send(int tableID,
    EdgePartition<I, E> partition, String host, int port, int senderID,
    ResourcePool resourcePool) {
    List<ByteArray> arrays = partition.getData();
    for (int i = 0; i < arrays.size(); i++) {
      ByteArray array = arrays.get(i);
      // int[] metaData = new int[3];
      int[] metaArray = resourcePool.getIntArrayPool().getArray(3);
      metaArray[0] = senderID;
      metaArray[1] = tableID;
      metaArray[2] = partition.getPartitionID();
      array.setMetaArray(metaArray);
      array.setMetaArraySize(3);
      // Byte array sending
      ByteArrReqSender sender = new ByteArrReqSender(host, port, array,
        resourcePool);
      sender.execute();
      // Once data are sent, no use for meta data
      resourcePool.getIntArrayPool().releaseArrayInUse(metaArray);
      array.setMetaArray(null);
      array.setMetaArraySize(0);
    }
  }

  public static <P extends StructPartition, T extends StructTable<P>> void allgatherVtx(
    Workers workers, WorkerData workerData, ResourcePool resourcePool, T table)
    throws IOException {
    if (workers.getNumWorkers() == 1) {
      return;
    }
    // Use allgather one to get the total partitions to receive
    // Create table, use -1 * tableID to avoid conflict internally
    // We may provide a fixed way to create table ID in future
    ArrTable<DoubleArray, DoubleArrPlus> arrTable = new ArrTable<DoubleArray, DoubleArrPlus>(
      table.getTableID() * -1, DoubleArray.class, DoubleArrPlus.class);
    // Create DoubleArray
    // Get double[1];
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
      return;
    }
    try {
      AllgatherWorker.allgatherOne(workers, workerData, resourcePool, arrTable);
    } catch (Exception e) {
      LOG.error("Fail to do allreduce one.", e);
      return;
    }
    DoubleArray outArray = arrTable.getPartitions()[0].getArray();
    int totalParRecv = (int) outArray.getArray()[0];
    resourcePool.getDoubleArrayPool().releaseArrayInUse(outArray.getArray());
    LOG.info("total partitions to receive: " + totalParRecv);
    // ------------------------------------------------------------------------
    int numDeserialThreads = Constants.NUM_DESERIAL_THREADS;
    // Original catcher
    // StructParCatcher<P, T> catcher = new StructParCatcher<P, T>(workers,
    // workerData,
    // resourcePool, totalParRecv, table, numDeserialThreads);
    // Partitions are put in one request
    MultiStructParCatcher<P, T> catcher = new MultiStructParCatcher<P, T>(
      workers, workerData, resourcePool, totalParRecv, table,
      numDeserialThreads);
    // Partitions are sent in multithreads
    // int numSendThreads = Constants.NUM_SENDER_THREADS;
    // StructParMultiThreadCatcher<P, T> catcher = new
    // StructParMultiThreadCatcher<P, T>(
    // workers, workerData, resourcePool, totalParRecv, table, numSendThreads,
    // numDeserialThreads);
    boolean success = catcher.waitAndGet();
    if (!success) {
      throw new IOException("Fail to catch all partitions in allgather");
    }
  }
  
  public static <P extends StructPartition, T extends StructTable<P>> void allgatherVtxTotalKnown(
    Workers workers, WorkerData workerData, ResourcePool resourcePool, T table,
    int totalNumPartitions) throws IOException {
    if (workers.getNumWorkers() == 1) {
      return;
    }
    // ------------------------------------------------------------------------
    int numDeserialThreads = Constants.NUM_DESERIAL_THREADS;
    // Partitions are put in one request
    MultiStructParCatcher<P, T> catcher = new MultiStructParCatcher<P, T>(
      workers, workerData, resourcePool, totalNumPartitions, table,
      numDeserialThreads);
    boolean success = catcher.waitAndGet();
    if (!success) {
      throw new IOException("Fail to catch all partitions in allgather");
    }
  }
}
