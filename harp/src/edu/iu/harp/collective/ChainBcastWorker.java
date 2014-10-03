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

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.arrpar.DoubleArrPlus;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.WorkerInfo;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.ByteArrReqSender;
import edu.iu.harp.comm.client.IntArrReqSender;
import edu.iu.harp.comm.client.ReqSender;
import edu.iu.harp.comm.client.StructObjReqSender;
import edu.iu.harp.comm.client.WritableObjReqSender;
import edu.iu.harp.comm.client.chainbcast.ByteArrChainBcastMaster;
import edu.iu.harp.comm.client.chainbcast.ChainBcastMaster;
import edu.iu.harp.comm.client.chainbcast.ChainBcastSlave;
import edu.iu.harp.comm.client.chainbcast.DblArrChainBcastMaster;
import edu.iu.harp.comm.client.chainbcast.IntArrChainBcastMaster;
import edu.iu.harp.comm.client.chainbcast.StructObjChainBcastMaster;
import edu.iu.harp.comm.client.chainbcast.WritableObjChainBcastMaster;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.data.IntMatrix;
import edu.iu.harp.comm.data.StructObject;
import edu.iu.harp.comm.data.WritableObject;
import edu.iu.harp.comm.request.ChainBcastAck;
import edu.iu.harp.comm.request.ReqAck;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.Receiver;

public class ChainBcastWorker extends CollCommWorker {

  public static void main(String args[]) throws Exception {
    String driverHost = args[0];
    int driverPort = Integer.parseInt(args[1]);
    int workerID = Integer.parseInt(args[2]);
    long jobID = Long.parseLong(args[3]);
    int totalByteSize = Integer.parseInt(args[4]);
    int numLoops = Integer.parseInt(args[5]);
    // Initialize log
    initLogger(workerID);
    LOG.info("args[] " + driverHost + " " + driverPort + " " + workerID + " "
      + jobID + " " + totalByteSize + " " + numLoops);
    // --------------------------------------------------------------
    try {
      runChainBcast(driverHost, driverPort, workerID, jobID, totalByteSize,
        numLoops);
    } catch (Exception e) {
      LOG.error("Fail to run bcast", e);
    }
  }

  public static void runChainBcast(String driverHost, int driverPort,
    int workerID, long jobID, int totalByteSize, int numLoops) throws Exception {
    // --------------------------------------------------------------------
    // Worker initialize
    LOG.info("0");
    Workers workers = new Workers(workerID);
    LOG.info("1");
    String host = workers.getSelfInfo().getNode();
    LOG.info("2");
    int port = workers.getSelfInfo().getPort();
    LOG.info("3");
    WorkerData workerData = new WorkerData();
    LOG.info("4");
    ResourcePool resourcePool = new ResourcePool();
    LOG.info("5");
    Receiver receiver = new Receiver(workerData, resourcePool, workers, host,
      port, Constants.NUM_HANDLER_THREADS);
    LOG.info("6");
    receiver.start();
    LOG.info("7");
    // Master check if all slaves are ready
    boolean success = masterHandshake(workers, workerData, resourcePool);
    LOG.info("Barrier: " + success);
    //-----------------------------------------------------------------
    if (success) {
      // Byte array bcast, send/recv
      
      // ByteArray byteArray = generateByteArray(workers, totalByteSize, false);
      // chainBcastBenchmark(byteArray, workers, workerData, resourcePool, numLoops);
      // sendRecvReq(byteArray, workerData, workers, resourcePool, numLoops);
      
      // DoubleArray doubleArray = generateDoubleArray(workers, totalByteSize);
      // chainBcastBenchmark(doubleArray, workers, workerData, resourcePool, numLoops);
      
      ArrTable<DoubleArray, DoubleArrPlus> arrTable = new ArrTable<DoubleArray, DoubleArrPlus>(
        0, DoubleArray.class, DoubleArrPlus.class);
      if (workers.isMaster()) {
        for (int i = 0; i < 8; i++) {
          DoubleArray doubleArray = generateDoubleArray(workers,
            totalByteSize / 8);
          arrTable.addPartition(new ArrPartition<DoubleArray>(doubleArray, i));
        }
      }
      for (int i = 0; i < numLoops; i++) {
        long start = System.currentTimeMillis();
        arrTableBcast(arrTable, workers, workerData, resourcePool);
        long end = System.currentTimeMillis();
        LOG.info("Total array table bcast time: " + (end - start));
        // If not master, release
        if (!workers.isMaster()) {
          for (ArrPartition<DoubleArray> partition : arrTable.getPartitions()) {
            resourcePool.getDoubleArrayPool().releaseArrayInUse(
              partition.getArray().getArray());
          }
          arrTable = new ArrTable<DoubleArray, DoubleArrPlus>(0,
            DoubleArray.class, DoubleArrPlus.class);
        }
      }
      
      /*
       * for (ArrPartition<DoubleArray> partition : arrTable.getPartitions())
       * { double[] doubles = partition.getArray().getArray();
       * LOG.info("First double: " + doubles[0] + ", last double: " +
       * doubles[partition.getArray().getSize() - 1]); }
       */

      /*
      try {
        long start = System.currentTimeMillis();
        prmtvArrChainBcast(doubleArray, workers, workerData, resourcePool);
        long end = System.currentTimeMillis();
        LOG.info("Double array");
        LOG.info("Receive double array with size: " + doubleArray.getSize());
        LOG.info("First double: " + doubleArray.getArray()[0]);
        LOG.info("Last double: "
          + doubleArray.getArray()[doubleArray.getSize() - 1]);
        resourcePool.getDoubleArrayPool().releaseArrayInUse(
          doubleArray.getArray());
      } catch (Exception e) {
        LOG.error("Fail to bcast.", e);
      }
      */
     
      //chainBcastBenchmark(doubleArray, workers, workerData, resourcePool,
      // numLoops);
      // IntArray intArray = generateIntArray(workers, totalByteSize);
      // chainBcast(intArray, workers, workerData, resourcePool, numLoops);
      // IntMatrix intMatrix = generateIntMatrix(workers, totalByteSize);
      // chainBcast(intMatrix, workers, workerData, resourcePool, numLoops);
    }
    reportWorkerStatus(resourcePool, workerID, driverHost, driverPort);
    receiver.stop();
    System.exit(0);
  }

  public static ByteArray generateByteArray(Workers workers, int totalByteData,
    boolean withMetaData) {
    ByteArray byteArray = null;
    if (workers.isMaster()) {
      long a = System.currentTimeMillis();
      byte[] bytes = new byte[totalByteData];
      bytes[0] = (byte) (Math.random() * 255);
      bytes[bytes.length - 1] = (byte) (Math.random() * 255);
      LOG.info("First byte: " + bytes[0] + ", Last byte: "
        + bytes[bytes.length - 1]);
      byteArray = new ByteArray();
      byteArray.setArray(bytes);
      byteArray.setSize(totalByteData);
      if (withMetaData) {
        int[] metaData = new int[1];
        metaData[0] = 1;
        byteArray.setMetaArray(metaData);
      }
      LOG.info("Byte array data generation time: "
        + (System.currentTimeMillis() - a));
    } else {
      // Empty byte array, a reference for receiving in bcast
      byteArray = new ByteArray();
    }
    return byteArray;
  }

  @SuppressWarnings("unused")
  private static IntArray generateIntArray(Workers workers, int totalByteData) {
    IntArray intArray = null;
    if (workers.isMaster()) {
      long start = System.currentTimeMillis();
      int size = (int) (totalByteData / (double) 4);
      int[] ints = new int[size];
      for (int i = 0; i < size; i++) {
        ints[i] = (int) (Math.random() * 255);
      }
      LOG
        .info("First int: " + ints[0] + ", Last int: " + ints[ints.length - 1]);
      intArray = new IntArray();
      intArray.setArray(ints);
      intArray.setSize(size);
      LOG.info("Int array data generation time: "
        + (System.currentTimeMillis() - start));
    } else {
      intArray = new IntArray();
    }
    return intArray;
  }

  private static DoubleArray generateDoubleArray(Workers workers,
    int totalByteData) {
    DoubleArray doubleArray = null;
    long start = System.currentTimeMillis();
    int size = (int) (totalByteData / (double) 8);
    double[] doubles = new double[size];
    doubles[0] = Math.random() * 255;
    doubles[doubles.length - 1] = Math.random() * 1000;
    LOG.info("First double: " + doubles[0] + ", last double: "
      + doubles[doubles.length - 1]);
    doubleArray = new DoubleArray();
    doubleArray.setArray(doubles);
    doubleArray.setSize(size);
    long end = System.currentTimeMillis();
    LOG.info("Double array data generation time: " + (end - start));
    return doubleArray;
  }

  @SuppressWarnings("unused")
  private static IntMatrix generateIntMatrix(Workers workers, int totalByteData) {
    IntMatrix matrix = null;
    if (workers.isMaster()) {
      long start = System.currentTimeMillis();
      int matrixLen = (int) Math.sqrt(totalByteData / (double) 4);
      matrix = new IntMatrix();
      int[][] matrixBody = new int[matrixLen][matrixLen];
      matrixBody[matrixLen - 1][matrixLen - 1] = 1;
      matrix.updateMatrix(matrixBody, matrixLen, matrixLen);
      LOG.info("Int matrix data generation time: "
        + (System.currentTimeMillis() - start));
    } else {
      matrix = new IntMatrix();
    }
    return matrix;
  }

  public static void chainBcastBenchmark(Commutable sendData, Workers workers,
    WorkerData workerData, ResourcePool resourcePool, int numLoops) {
    // ByteArray Chain broadcaster
    for (int i = 0; i < numLoops; i++) {
      LOG.info("START CHAIN BCAST, LOOP " + i);
      if (workers.isMaster()) {
        long bcastStartTime = System.currentTimeMillis();
        ChainBcastMaster bacbDriver;
        try {
          bacbDriver = getChainBcastMaster(sendData, workers, resourcePool);
          bacbDriver.execute();
          LOG.info("Bcast time: " + (System.currentTimeMillis() - bcastStartTime));
          examineBcastACK(workerData, resourcePool, bcastStartTime);
        } catch (Exception e) {
          LOG.error("Exception in bcasting data.", e);
        }
      } else {
        // For slaves, sendData is empty, but it could be used as reference in
        // real cases
        Commutable recvData = workerData
          .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
        // Reply quickly for benchmarking
        if (workers.isMax()) {
          ChainBcastSlave.sendACK(workers.getMasterInfo().getNode(), workers
            .getMasterInfo().getPort());
        }
        examineBcastRecvData(sendData, recvData, resourcePool);
      }
    }
  }

  private static ChainBcastMaster getChainBcastMaster(Commutable data,
    Workers workers, ResourcePool resourcePool) throws Exception {
    if (data instanceof ByteArray) {
      return new ByteArrChainBcastMaster((ByteArray) data, workers,
        resourcePool);
    } else if (data instanceof IntArray) {
      return new IntArrChainBcastMaster((IntArray) data, workers, resourcePool);
    } else if (data instanceof StructObject) {
      return new StructObjChainBcastMaster((StructObject) data, workers,
        resourcePool);
    } else if (data instanceof DoubleArray) {
      return new DblArrChainBcastMaster((DoubleArray) data, workers,
        resourcePool);
    } else {
      return new WritableObjChainBcastMaster((WritableObject) data, workers,
        resourcePool);
    }
  }

  private static void examineBcastACK(WorkerData workerData,
    ResourcePool resourcePool, long bcastStartTime) {
    Commutable data = workerData
      .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
    if (data != null) {
      if (data instanceof ChainBcastAck) {
        ChainBcastAck ack = (ChainBcastAck) data;
        LOG.info("Bcast time with ACK: " + (System.currentTimeMillis() - bcastStartTime)
          + " Bcast ACK ID: " + ack.getAckID());
        resourcePool.getWritableObjectPool().releaseWritableObjectInUse(ack);
      } else {
        LOG.info("IRRELEVANT data: " + data.getClass().getName());
      }
    } else {
      LOG.info("No data received after MAX WAIT TIME.");
    }
  }

  private static void examineBcastRecvData(Commutable sendData,
    Commutable recvData, ResourcePool resourcePool) {
    if ((sendData instanceof ByteArray) && (recvData instanceof ByteArray)) {
      ByteArray recvByteArray = (ByteArray) recvData;
      LOG.info("Receive byte array with size: " + recvByteArray.getSize()
        + " First element: " + recvByteArray.getArray()[0] + " Last element: "
        + recvByteArray.getArray()[recvByteArray.getSize() - 1]);
      int[] metaData = recvByteArray.getMetaArray();
      if (metaData != null) {
        LOG.info("metaData size: " + metaData.length + ",metaData value: "
          + metaData[0]);
      } else {
        LOG.info("metaData size: NULL");
      }
      resourcePool.getByteArrayPool().releaseArrayInUse(
        recvByteArray.getArray());
      resourcePool.getIntArrayPool().releaseArrayInUse(
        recvByteArray.getMetaArray());
    } else if ((sendData instanceof IntArray) && (recvData instanceof IntArray)) {
      IntArray recvIntArray = (IntArray) recvData;
      LOG.info("Receive int array with size: " + recvIntArray.getSize());
      LOG.info("First int: " + recvIntArray.getArray()[0]);
      LOG.info("Last int: "
        + recvIntArray.getArray()[recvIntArray.getSize() - 1]);
      resourcePool.getIntArrayPool().releaseArrayInUse(recvIntArray.getArray());
    } else if ((sendData instanceof DoubleArray)
      && (recvData instanceof DoubleArray)) {
      DoubleArray recvDoubleArray = (DoubleArray) recvData;
      LOG.info("Receive double array with size: " + recvDoubleArray.getSize());
      LOG.info("First double: " + recvDoubleArray.getArray()[0]);
      LOG.info("Last double: "
        + recvDoubleArray.getArray()[recvDoubleArray.getSize() - 1]);
      resourcePool.getDoubleArrayPool().releaseArrayInUse(
        recvDoubleArray.getArray());
    } else if ((sendData instanceof StructObject)
      && (recvData instanceof StructObject)) {
      // IntMatrix is a struct object
      IntMatrix matrix = (IntMatrix) recvData;
      LOG.info("intMatrixChainBcast: Receive matrix with row: "
        + matrix.getRow() + " col: " + matrix.getCol());
      LOG.info("Last cell in IntMatrix: "
        + matrix.getMatrixBody()[matrix.getRow() - 1][matrix.getCol() - 1]);
      resourcePool.getWritableObjectPool().releaseWritableObjectInUse(matrix);
    } else {
      LOG.info("Irrelevant data is received.");
    }
    LOG.info("CHAIN BCAST SLAVE FINISHES.");
  }

  @SuppressWarnings("unused")
  private static void sendRecvReq(Commutable sendData, WorkerData workerData,
    Workers workers, ResourcePool resourcePool, int numLoops) {
    WorkerInfo nextWorkerInfo = workers.getNextInfo();
    String nextHost = nextWorkerInfo.getNode();
    int nextPort = nextWorkerInfo.getPort();
    for (int i = 0; i < numLoops; i++) {
      LOG.info("START SEND/RECV REQ, LOOP " + i);
      if (workers.isMaster()) {
        long reqStartTime = System.currentTimeMillis();
        ReqSender sender = getRequestSender(nextHost, nextPort, sendData,
          resourcePool);
        sender.execute();
        LOG.info("Send request time: "
          + (System.currentTimeMillis() - reqStartTime));
        Commutable data = workerData
          .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
        examineReqACK(data, resourcePool, reqStartTime);
      } else if (workers.getSelfID() == workers.getMasterID() + 1) {
        Commutable recvData = workerData
          .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
        sendReqACKtoMaster(workers, resourcePool);
        examineReqRecvData(sendData, recvData, resourcePool);
      }
    }
  }

  private static ReqSender getRequestSender(String host, int port,
    Commutable data, ResourcePool resourcePool) {
    if (data instanceof ByteArray) {
      return new ByteArrReqSender(host, port, (ByteArray) data, resourcePool);
    } else if (data instanceof IntArray) {
      return new IntArrReqSender(host, port, (IntArray) data, resourcePool);
    } else if (data instanceof StructObject) {
      return new StructObjReqSender(host, port, (StructObject) data,
        resourcePool);
    } else {
      return new WritableObjReqSender(host, port, (WritableObject) data,
        resourcePool);
    }
  }

  private static void examineReqACK(Commutable data, ResourcePool resourcePool,
    long reqStartTime) {
    if (data != null) {
      if (data instanceof ReqAck) {
        ReqAck ack = (ReqAck) data;
        LOG.info("Receive request ACK: " + ack.getAckID());
        LOG.info("Send request time with ACK: "
          + (System.currentTimeMillis() - reqStartTime));
        resourcePool.getWritableObjectPool().releaseWritableObjectInUse(ack);
      } else {
        LOG.info("IRRELEVANT data: " + data.getClass().getName());
      }
    } else {
      LOG.info("No data is received after MAX WAIT TIME.");
    }
  }

  private static void examineReqRecvData(Commutable sendData,
    Commutable recvData, ResourcePool resourcePool) {
    if ((sendData instanceof ByteArray) && (recvData instanceof ByteArray)) {
      ByteArray byteArray = (ByteArray) recvData;
      LOG.info("Receive byte array with size: " + byteArray.getSize());
      resourcePool.getByteArrayPool().releaseArrayInUse(byteArray.getArray());
      resourcePool.getIntArrayPool()
        .releaseArrayInUse(byteArray.getMetaArray());
    } else if ((sendData instanceof IntArray) && (recvData instanceof IntArray)) {
      IntArray intArray = (IntArray) recvData;
      LOG.info("Receive int array with size: " + intArray.getSize());
      resourcePool.getIntArrayPool().releaseArrayInUse(intArray.getArray());
    } else if ((sendData instanceof DoubleArray)
      && (recvData instanceof DoubleArray)) {
      DoubleArray doubleArray = (DoubleArray) recvData;
      LOG.info("Receive double array with size: " + doubleArray.getSize());
      resourcePool.getDoubleArrayPool().releaseArrayInUse(
        doubleArray.getArray());
    } else if ((sendData instanceof StructObject)
      && (recvData instanceof StructObject)) {
      // an Int matrix is a struct object
      IntMatrix matrix = (IntMatrix) recvData;
      LOG.info("receiveIntMatrix, Receive matrix with row: " + matrix.getRow()
        + " col: " + matrix.getCol() + " size in bytes: "
        + matrix.getSizeInBytes());
      LOG.info("Last cell in IntMatrix: "
        + matrix.getMatrixBody()[matrix.getRow() - 1][matrix.getCol() - 1]);
      resourcePool.getWritableObjectPool().releaseWritableObjectInUse(matrix);
    } else {
      LOG.info("IRRELEVANT data: " + recvData.getClass().getName());
    }
  }

  private static void sendReqACKtoMaster(Workers workers,
    ResourcePool resourcePool) {
    String masterHost = workers.getMasterInfo().getNode();
    int masterPort = workers.getMasterInfo().getPort();
    ReqAck ack = (ReqAck) resourcePool.getWritableObjectPool()
      .getWritableObject(ReqAck.class.getName());
    ack.setWorkerID(workers.getSelfID());
    ack.setAckID(ack.getAckID() + 1);
    StructObjReqSender sender = new StructObjReqSender(masterHost, masterPort,
      ack, resourcePool);
    sender.execute();
    resourcePool.getWritableObjectPool().releaseWritableObjectInUse(ack);
  }
}
