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

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import edu.iu.harp.arrpar.ArrCombiner;
import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.comm.CommUtil;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.WorkerInfo;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.client.ReqSender;
import edu.iu.harp.comm.client.StructObjReqSender;
import edu.iu.harp.comm.client.allbcast.ArrTableBcastSlave;
import edu.iu.harp.comm.client.allbcast.DblArrParBcastMaster;
import edu.iu.harp.comm.client.allbcast.StructParBcastMaster;
import edu.iu.harp.comm.client.allbcast.StructTableBcastSlave;
import edu.iu.harp.comm.client.chainbcast.ByteArrChainBcastMaster;
import edu.iu.harp.comm.client.chainbcast.ChainBcastMaster;
import edu.iu.harp.comm.client.chainbcast.ChainBcastSlave;
import edu.iu.harp.comm.client.chainbcast.DblArrChainBcastMaster;
import edu.iu.harp.comm.client.chainbcast.IntArrChainBcastMaster;
import edu.iu.harp.comm.client.chainbcast.StructObjChainBcastMaster;
import edu.iu.harp.comm.client.chainbcast.WritableObjChainBcastMaster;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.data.ByteArray;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.data.IntArray;
import edu.iu.harp.comm.data.StructObject;
import edu.iu.harp.comm.data.WritableObject;
import edu.iu.harp.comm.request.AllbcastReq;
import edu.iu.harp.comm.request.AllgatherReq;
import edu.iu.harp.comm.request.ChainBcastAck;
import edu.iu.harp.comm.request.ReqAck;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.graph.vtx.StructPartition;
import edu.iu.harp.graph.vtx.StructTable;

public class CollCommWorker {
  /** Class logger */
  protected static final Logger LOG = Logger.getLogger(CollCommWorker.class);

  private static ExecutorService taskExecutor = Executors.newCachedThreadPool();

  /**
   * It seems that the logger you get in static field can be re-initialized
   * later.
   * 
   * @param workerID
   */
  protected static void initLogger(int workerID) {
    String fileName = "harp-worker-" + workerID + ".log";
    File file = new File(fileName);
    if (file.exists()) {
      file.delete();
    }
    FileAppender fileAppender = new FileAppender();
    fileAppender.setName("FileLogger");
    fileAppender.setFile(fileName);
    fileAppender.setLayout(new PatternLayout(
      "%d{dd MMM yyyy HH:mm:ss,SSS} %-4r [%t] %-5p %c %x - %m%n"));
    fileAppender.setAppend(true);
    fileAppender.activateOptions();
    // Add appender to any Logger (here is root)
    Logger.getRootLogger().addAppender(fileAppender);
  }

  /**
   * Master side handshake, master decides if it can lead the system to the next
   * stage, if no, it stops. A slave may get the wrong view of the barrier.But
   * as long as it doesn;t any command in future, it can stop itself.
   * 
   * @param workers
   * @param workerData
   * @param resourcePool
   * @return
   */
  public static boolean masterHandshake(Workers workers, WorkerData workerData,
    ResourcePool resourcePool) {
    if(workers.getNumWorkers() == 1) {
      return true;
    }
    boolean success = true;
    // Do three handshakes
    if (workers.isMaster()) {
      // Initialize counter
      int[] counter = new int[workers.getNumWorkers()];
      Barrier barrier = (Barrier) resourcePool.getWritableObjectPool()
        .getWritableObject(Barrier.class.getName());
      // Enter the barrier
      // First handshake
      BlockingQueue<WorkerInfo> queue = new ArrayBlockingQueue<WorkerInfo>(
        workers.getNumWorkers());
      for (WorkerInfo worker : workers.getWorkerInfoList()) {
        if (worker.getID() != workers.getMasterID()) {
          queue.add(worker);
        }
      }
      while (!queue.isEmpty()) {
        WorkerInfo worker = queue.poll();
        ReqSender sender = new StructObjReqSender(worker.getNode(),
          worker.getPort(), barrier, resourcePool);
        success = sender.execute();
        // Retry
        if (!success && counter[worker.getID()] < Constants.RETRY_COUNT) {
          queue.add(worker);
          counter[worker.getID()]++;
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            LOG.error("Error in thread sleep.", e);
          }
        } else if (counter[worker.getID()] == Constants.RETRY_COUNT) {
          // Release before return
          resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
            barrier);
          return false;
        }
      }
      resourcePool.getWritableObjectPool().releaseWritableObjectInUse(barrier);
      // Second handshake
      LOG.info("Barrier: collect responses from slaves.");
      for (int i = 1; i < counter.length; i++) {
        WorkerStatus status = (WorkerStatus) workerData
          .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
        if (status != null) {
          LOG.info("Barrier: Slave " + status.getWorkerID());
          resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
            status);
        } else {
          LOG.info("At least one slave may fail.");
          return false;
        }
      }
    } else {
      // View from a slave
      // Second handshake
      Barrier barrier = (Barrier) workerData
        .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (barrier != null) {
        LOG.info("Barrier: Send worker status. " + workers.getSelfID());
        resourcePool.getWritableObjectPool()
          .releaseWritableObjectInUse(barrier);
        WorkerStatus status = (WorkerStatus) resourcePool
          .getWritableObjectPool().getWritableObject(
            WorkerStatus.class.getName());
        status.setWorkerID(workers.getSelfID());
        ReqSender sender = new StructObjReqSender(workers.getMasterInfo()
          .getNode(), workers.getMasterInfo().getPort(), status, resourcePool);
        success = sender.execute();
        resourcePool.getWritableObjectPool().releaseWritableObjectInUse(status);
        if (!success) {
          return false;
        }
      }
    }
    // Third handshake
    Barrier barrier = null;
    if (workers.isMaster()) {
      barrier = (Barrier) resourcePool.getWritableObjectPool()
        .getWritableObject(Barrier.class.getName());
      try {
        ChainBcastMaster bcastDriver = new StructObjChainBcastMaster(barrier,
          workers, resourcePool);
        success = bcastDriver.execute();
        resourcePool.getWritableObjectPool()
          .releaseWritableObjectInUse(barrier);
        if (!success) {
          return false;
        }
      } catch (Exception e) {
        LOG.error("EXCEPTION IN BCAST BARRIER.", e);
        return false;
      }
    } else {
      // Wait for data
      barrier = CommUtil.waitAndGet(workerData, Barrier.class,
        Constants.DATA_MAX_WAIT_TIME, 500);
      if (barrier == null) {
        LOG.error("No BARRIER RECV AFTER MAX WAIT TIME.");
        return false;
      }
      resourcePool.getWritableObjectPool().releaseWritableObjectInUse(barrier);
    }
    return success;
  }
  
  /**
   * Barrier at master side
   * 
   * @param workers
   * @param workerData
   * @param resourcePool
   * @return
   */
  public static boolean masterBarrier(Workers workers, WorkerData workerData,
    ResourcePool resourcePool) {
    // do collect first
    boolean success = true;
    if(workers.getNumWorkers() == 1) {
      return success;
    }
    if (workers.isMaster()) {
      int numWorkers = workers.getNumWorkers();
      for (int i = 1; i < numWorkers; i++) {
        WorkerStatus status = 
        CommUtil.waitAndGet(workerData, WorkerStatus.class,
          Constants.DATA_MAX_WAIT_TIME, 100);
        if (status != null) {
          resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
            status);
        } else {
          return false;
        }
      }
    } else {
      WorkerStatus status = (WorkerStatus) resourcePool
        .getWritableObjectPool().getWritableObject(
          WorkerStatus.class.getName());
      status.setWorkerID(workers.getSelfID());
      StructObjReqSender sender = new StructObjReqSender(workers
        .getMasterInfo().getNode(), workers.getMasterInfo().getPort(), status,
        resourcePool);
      success = sender.execute();
      resourcePool.getWritableObjectPool().releaseWritableObjectInUse(status);
      if (!success) {
        return false;
      }
    }
    // then bcast
    Barrier barrier = null;
    if (workers.isMaster()) {
      barrier = (Barrier) resourcePool.getWritableObjectPool()
        .getWritableObject(Barrier.class.getName());
      try {
        ChainBcastMaster bcastDriver = new StructObjChainBcastMaster(barrier,
          workers, resourcePool);
        success = bcastDriver.execute();
        resourcePool.getWritableObjectPool()
          .releaseWritableObjectInUse(barrier);
        if (!success) {
          return false;
        }
      } catch (Exception e) {
        LOG.error("EXCEPTION IN BCAST BARRIER.", e);
        return false;
      }
    } else {
      // Wait for data
      barrier = CommUtil.waitAndGet(workerData, Barrier.class,
        Constants.DATA_MAX_WAIT_TIME, 500);
      if (barrier == null) {
        LOG.error("No BARRIER RECV AFTER MAX WAIT TIME.");
        return false;
      }
      resourcePool.getWritableObjectPool().releaseWritableObjectInUse(barrier);
    }
    return true;
  }

  /**
   * Report to driver that worker
   */
  protected static void reportWorkerStatus(ResourcePool resourcePool,
    int workerID, String driverHost, int driverPort) {
    LOG.info("Worker " + workerID + " reports. ");
    // Send ack to report worker status
    WorkerStatus ack = (WorkerStatus) resourcePool.getWritableObjectPool()
      .getWritableObject(WorkerStatus.class.getName());
    ack.setWorkerID(workerID);
    ReqSender sender = new StructObjReqSender(driverHost, driverPort, ack,
      resourcePool);
    sender.execute();
  }

  public static <T, A extends Array<T>> boolean prmtvArrChainBcast(A array,
    Workers workers, WorkerData workerData, ResourcePool resourcePool) {
    if (workers.isMaster()) {
      ChainBcastMaster bcastDriver;
      try {
        if (array instanceof ByteArray) {
          bcastDriver = new ByteArrChainBcastMaster(array, workers,
            resourcePool);
          bcastDriver.execute();
        } else if (array instanceof IntArray) {
          bcastDriver = new IntArrChainBcastMaster(array, workers, resourcePool);
          bcastDriver.execute();
        } else if (array instanceof DoubleArray) {
          bcastDriver = new DblArrChainBcastMaster(array, workers, resourcePool);
          bcastDriver.execute();
        } else {
          LOG.error("Fail to get bcaster type: " + array.getClass().getName());
        }
      } catch (Exception e) {
        LOG.error("Exception in bcasting data.", e);
      }
      Commutable data = workerData
        .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data != null) {
        if (data instanceof ChainBcastAck) {
          resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
            (ChainBcastAck) data);
          return true;
        } else {
          LOG.error("IRRELEVANT receiving data in bcast: "
            + data.getClass().getName());
        }
      } else {
        LOG.error("No Bcast ACK after MAX WAIT TIME.");
      }
      return false;
    } else {
      Commutable data = workerData
        .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data != null) {
        if (data.getClass().equals(array.getClass())) {
          @SuppressWarnings("unchecked")
          A recvArray = (A) data;
          array.setArray(recvArray.getArray());
          array.setSize(recvArray.getSize());
          if (workers.isMax()) {
            ChainBcastSlave.sendACK(workers.getMasterInfo().getNode(), workers
              .getMasterInfo().getPort());
          }
          return true;
        } else {
          LOG.error("IRRELEVANT data: " + data.getClass().getName());
        }
      }
      return false;
    }
  }

  public static <T, A extends Array<T>, C extends ArrCombiner<A>> boolean arrTableBcast(
    ArrTable<A, C> table, Workers workers, WorkerData workerData,
    ResourcePool resourcePool) {
    if (workers.getNumWorkers() == 1) {
      return true;
    }
    // Start array table bcast
    AllbcastReq[] allbcastReqRef = new AllbcastReq[1];
    if (workers.isMaster()) {
      AllbcastReq allbcastReq = new AllbcastReq(table.getNumPartitions());
      allbcastReqRef[0] = allbcastReq;
    }
    reqChainBcast(allbcastReqRef, workers, workerData, resourcePool,
      AllbcastReq.class);
    AllbcastReq allbcastReq = allbcastReqRef[0];
    int totalPartitions = allbcastReq.getTotalRecvParNum();
    resourcePool.getWritableObjectPool()
      .releaseWritableObjectInUse(allbcastReq);
    LOG.info("Total array partitions to receive in Allbcast: "
      + totalPartitions);
    if (workers.isMaster()) {
      Class<A> aClass = table.getAClass();
      ChainBcastMaster bcastDriver;
      try {
        if (aClass.equals(DoubleArray.class)) {
          ArrPartition<A>[] partitions = table.getPartitions();
          for (ArrPartition<A> partition : partitions) {
            ArrPartition<DoubleArray> dblPar = (ArrPartition<DoubleArray>) partition;
            bcastDriver = new DblArrParBcastMaster(dblPar, workers,
              resourcePool);
            bcastDriver.execute();
          }
        } else {
          LOG.error("Fail to get bcast array table: " + aClass.getName());
        }
      } catch (Exception e) {
        LOG.error("Exception in bcasting array table.", e);
      }
      ChainBcastAck ack = CommUtil.waitAndGet(workerData, ChainBcastAck.class,
        Constants.DATA_MAX_WAIT_TIME, 100);
      if (ack != null) {
        resourcePool.getWritableObjectPool().releaseWritableObjectInUse(ack);
        return true;
      } else {
        LOG.error("No Bcast ACK after MAX WAIT TIME.");
      }
      return false;
    } else {
      ArrTableBcastSlave<A, C> slave = new ArrTableBcastSlave<A, C>(workers,
        workerData, resourcePool, totalPartitions, table,
        Constants.NUM_DESERIAL_THREADS);
      boolean success = slave.waitAndGet();
      if (success && workers.isMax()) {
        ChainBcastSlave.sendACK(workers.getMasterInfo().getNode(), workers
          .getMasterInfo().getPort());
      }
      return success;
    }
  }

  public static <T, A extends Array<T>, C extends ArrCombiner<A>> boolean arrTableBcastTotalKnown(
    ArrTable<A, C> table, int totalPartitions, Workers workers,
    WorkerData workerData, ResourcePool resourcePool) {
    if (workers.getNumWorkers() == 1) {
      return true;
    }
    if (workers.isMaster()) {
      Class<A> aClass = table.getAClass();
      ChainBcastMaster bcastDriver;
      try {
        if (aClass.equals(DoubleArray.class)) {
          ArrPartition<A>[] partitions = table.getPartitions();
          for (ArrPartition<A> partition : partitions) {
            ArrPartition<DoubleArray> dblPar = (ArrPartition<DoubleArray>) partition;
            bcastDriver = new DblArrParBcastMaster(dblPar, workers,
              resourcePool);
            bcastDriver.execute();
          }
        } else {
          LOG.error("Fail to get bcast array table: " + aClass.getName());
        }
      } catch (Exception e) {
        LOG.error("Exception in bcasting array table.", e);
      }
      ChainBcastAck ack = CommUtil.waitAndGet(workerData, ChainBcastAck.class,
        Constants.DATA_MAX_WAIT_TIME, 100);
      if (ack != null) {
        resourcePool.getWritableObjectPool().releaseWritableObjectInUse(ack);
        return true;
      } else {
        LOG.error("No Bcast ACK after MAX WAIT TIME.");
      }
      return false;
    } else {
      ArrTableBcastSlave<A, C> slave = new ArrTableBcastSlave<A, C>(workers,
        workerData, resourcePool, totalPartitions, table,
        Constants.NUM_DESERIAL_THREADS);
      boolean success = slave.waitAndGet();
      if (success && workers.isMax()) {
        ChainBcastSlave.sendACK(workers.getMasterInfo().getNode(), workers
          .getMasterInfo().getPort());
      }
      return success;
    }
  }

  public static <P extends StructPartition, T extends StructTable<P>> boolean structTableBcast(
    T table, Workers workers, WorkerData workerData, ResourcePool resourcePool) {
    if (workers.getNumWorkers() == 1) {
      return true;
    }
    AllbcastReq[] allbcastReqRef = new AllbcastReq[1];
    AllbcastReq allbcastReq = null;
    if (workers.isMaster()) {
      allbcastReq = new AllbcastReq(table.getNumPartitions());
      allbcastReqRef[0] = allbcastReq;
    }
    reqChainBcast(allbcastReqRef, workers, workerData, resourcePool,
      AllbcastReq.class);
    allbcastReq = allbcastReqRef[0];
    int totalPartitions = allbcastReq.getTotalRecvParNum();
    // Try to free the object but not cache
    resourcePool.getWritableObjectPool().freeWritableObjectInUse(allbcastReq);
    LOG.info("Total struct partitions to receive in Allbcast: "
      + totalPartitions);
    if (workers.isMaster()) {
      ChainBcastMaster bcastDriver;
      try {
        P[] partitions = table.getPartitions();
        for (P partition : partitions) {
          bcastDriver = new StructParBcastMaster<P>(workers, resourcePool,
            partition);
          boolean success = bcastDriver.execute();
          if (!success) {
            LOG.error("Fail in bcasting struct table.");
          }
        }
      } catch (Exception e) {
        LOG.error("Exception in bcasting struct table.", e);
      }
      ChainBcastAck ack = CommUtil.waitAndGet(workerData, ChainBcastAck.class,
        Constants.DATA_MAX_WAIT_TIME, 500);
      if (ack != null) {
        resourcePool.getWritableObjectPool().releaseWritableObjectInUse(ack);
        return true;
      } else {
        LOG.error("No Bcast ACK after MAX WAIT TIME.");
      }
      return false;
    } else {
      StructTableBcastSlave<P, T> slave = new StructTableBcastSlave<P, T>(
        workers, workerData, resourcePool, totalPartitions, table, 2);
      boolean success = slave.waitAndGet();
      if (success && workers.isMax()) {
        ChainBcastSlave.sendACK(workers.getMasterInfo().getNode(), workers
          .getMasterInfo().getPort());
      }
      return success;
    }
  }

  public static boolean chainBcast(Commutable[] sendDataRef, Workers workers,
    WorkerData workerData, ResourcePool resourcePool) {
    if (workers.isMaster()) {
      ChainBcastMaster bcastDriver;
      try {
        if (sendDataRef[0] instanceof ByteArray) {
          bcastDriver = new ByteArrChainBcastMaster((ByteArray) sendDataRef[0],
            workers, resourcePool);
          bcastDriver.execute();
        } else if (sendDataRef[0] instanceof IntArray) {
          bcastDriver = new IntArrChainBcastMaster((IntArray) sendDataRef[0],
            workers, resourcePool);
          bcastDriver.execute();
        } else if (sendDataRef[0] instanceof DoubleArray) {
          bcastDriver = new DblArrChainBcastMaster(
            (DoubleArray) sendDataRef[0], workers, resourcePool);
          bcastDriver.execute();
        } else if (sendDataRef[0] instanceof StructObject) {
          bcastDriver = new StructObjChainBcastMaster(
            (StructObject) sendDataRef[0], workers, resourcePool);
          bcastDriver.execute();
        } else if (sendDataRef[0] instanceof WritableObject) {
          bcastDriver = new WritableObjChainBcastMaster(
            (WritableObject) sendDataRef[0], workers, resourcePool);
          bcastDriver.execute();
        } else {
          LOG.error(" Fail to get bcaster type: "
            + sendDataRef[0].getClass().getName());
        }
      } catch (Exception e) {
        LOG.error("Exception in bcasting data.", e);
      }
      Commutable data = workerData
        .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data != null) {
        if (data instanceof ChainBcastAck) {
          resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
            (ChainBcastAck) data);
          return true;
        } else {
          LOG.info("IRRELEVANT ACK data: " + data.getClass().getName());
          if (data instanceof WorkerStatus) {
            LOG.info("IRRELEVANT Worker Stauts: "
              + ((WorkerStatus) data).getWorkerID());
          }
        }
      } else {
        LOG.info("No Bcast ACK after MAX WAIT TIME.");
      }
      return false;
    } else {
      Commutable recvData = workerData
        .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (recvData != null) {
        if (recvData.getClass().equals(
          sendDataRef.getClass().getComponentType())) {
          sendDataRef[0] = recvData;
          if (workers.isMax()) {
            ChainBcastSlave.sendACK(workers.getMasterInfo().getNode(), workers
              .getMasterInfo().getPort());
          }
          return true;
        } else {
          LOG.info("IRRELEVANT data: " + recvData.getClass().getName());
        }
      }
      return false;
    }
  }

  /**
   * There is no reply form the max worker in req chainbcast
   * 
   * @param dataRef
   * @param workers
   * @param workerData
   * @param resourcePool
   * @param sClass
   * @return
   */
  protected static <S extends StructObject> boolean reqChainBcast(S[] dataRef,
    Workers workers, WorkerData workerData, ResourcePool resourcePool,
    Class<S> sClass) {
    if (workers.isMaster()) {
      S sendData = dataRef[0];
      ChainBcastMaster bcastDriver = null;
      try {
        bcastDriver = new StructObjChainBcastMaster(sendData, workers,
          resourcePool);
        boolean success = bcastDriver.execute();
        if (!success) {
          return false;
        }
      } catch (Exception e) {
        LOG.error("Exception in bcasting data.", e);
        return false;
      }
      return true;
    } else {
      // Wait for data
      dataRef[0] = CommUtil.waitAndGet(workerData, sClass,
        Constants.DATA_MAX_WAIT_TIME, 500);
      if (dataRef[0] == null) {
        LOG.error("No BCAST REQ AFTER MAX WAIT TIME.");
        return false;
      }
      return true;
    }
  }

  @SuppressWarnings("unchecked")
  protected static <S extends StructObject> boolean reqGather(Workers workers,
    WorkerData workerData, S sendData, S[][] gatheredDataRef,
    ResourcePool resourcePool) {
    if (workers.isMaster()) {
      // long time1 = System.currentTimeMillis();
      // Bcast barrier
      Barrier barrier = (Barrier) resourcePool.getWritableObjectPool()
        .getWritableObject(Barrier.class.getName());
      try {
        StructObjChainBcastMaster bcastMaster = new StructObjChainBcastMaster(
          barrier, workers, resourcePool);
        bcastMaster.execute();
      } catch (Exception e) {
        LOG.error("Error when initializing bcast.", e);
      }
      // long time2 = System.currentTimeMillis();
      // LOG.info("Gather Data Barrier: " + (time2 - time1));
      resourcePool.getWritableObjectPool().releaseWritableObjectInUse(barrier);
      int numWorkers = workers.getNumWorkers();
      S[] gatheredData = (S[]) java.lang.reflect.Array.newInstance(
        sendData.getClass(), numWorkers);
      gatheredDataRef[0] = gatheredData;
      int count = 0;
      gatheredData[count] = sendData;
      count++;
      while (count < numWorkers) {
        // long time3 = System.currentTimeMillis();
        Commutable data = workerData
          .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
        if (data == null) {
          LOG.error("No gather data is received.");
          return false;
        } else if (data.getClass().equals(sendData.getClass())) {
          gatheredData[count] = (S) data;
          count++;
        } else {
          LOG.error("IRRELEVANT receiving data in gather: "
            + data.getClass().getName());
        }
        // long time4 = System.currentTimeMillis();
        // LOG.info("Gather Data Per Wait: " + (time4 - time3));
      }
      return true;
    } else {
      Commutable data = workerData
        .waitAndGetCommData(Constants.DATA_MAX_WAIT_TIME);
      if (data != null && data instanceof Barrier) {
        resourcePool.getWritableObjectPool().releaseWritableObjectInUse(
          (Barrier) data);
        StructObjReqSender sender = new StructObjReqSender(workers
          .getMasterInfo().getNode(), workers.getMasterInfo().getPort(),
          sendData, resourcePool);
        boolean success = sender.execute();
        return success;
      } else if (data != null) {
        LOG.error("IRRELEVANT sending data in gather: "
          + data.getClass().getName());
      } else {
        LOG.error("No barrier received in gather.");
      }
      return false;
    }
  }

  /**
   * No barrier compared with "gather" above
   * 
   * @param workers
   * @param workerData
   * @param sendData
   * @param gatheredDataRef
   * @param resourcePool
   * @return
   */
  protected static <S extends StructObject> boolean reqCollect(Workers workers,
    WorkerData workerData, S sendData, S[][] gatheredDataRef,
    ResourcePool resourcePool) {
    if (workers.isMaster()) {
      long time1 = System.currentTimeMillis();
      int numWorkers = workers.getNumWorkers();
      S[] gatheredData = (S[]) java.lang.reflect.Array.newInstance(
        sendData.getClass(), numWorkers);
      gatheredDataRef[0] = gatheredData;
      int count = 0;
      gatheredData[count] = sendData;
      count++;
      Class<S> sClass = (Class<S>) sendData.getClass();
      while (count < numWorkers) {
        S data = CommUtil.waitAndGet(workerData, sClass,
          Constants.DATA_MAX_WAIT_TIME, 100);
        if (data != null) {
          gatheredData[count] = (S) data;
          count++;
        } else {
          return false;
        }
      }
      long time2 = System.currentTimeMillis();
      LOG.info("Collect Wait: " + (time2 - time1));
      return true;
    } else {
      StructObjReqSender sender = new StructObjReqSender(workers
        .getMasterInfo().getNode(), workers.getMasterInfo().getPort(),
        sendData, resourcePool);
      boolean success = sender.execute();
      return success;
    }
  }

  public static <R, C extends Callable<R>> Set<R> doTasks(List<C> tasks,
    String taskName) {
    // Initialize executor
    ExecutorService taskExecutor = null;
    if (tasks.size() > 1) {
      taskExecutor = Executors.newFixedThreadPool(tasks.size());
    } else if (tasks.size() == 1) {
      taskExecutor = Executors.newSingleThreadExecutor();
    } else {
      return null;
    }
    // Execute
    Set<Future<R>> futureSet = new ObjectOpenHashSet<Future<R>>();
    Set<R> rSet = new ObjectOpenHashSet<R>(tasks.size());
    for (int i = 0; i < tasks.size(); i++) {
      Future<R> future = taskExecutor.submit(tasks.get(i));
      futureSet.add(future);
    }
    // Get result
    boolean success = true;
    for (Future<R> future : futureSet) {
      try {
        rSet.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("", e);
        success = false;
        break;
      }
    }
    // Shutdown
    CommUtil.closeExecutor(taskExecutor, taskName);
    if (!success) {
      return null;
    }
    return rSet;
  }

  public static <I, O, T extends Task<I, O>> List<O> doTasks(List<I> inputs,
    String taskName, T task, int numThreads) {
    if(inputs.size() == 0) {
      return null;
    }
    if (numThreads == 0) {
      numThreads = Runtime.getRuntime().availableProcessors();
    }
    if (numThreads > inputs.size()) {
      numThreads = inputs.size();
    }
    final BlockingQueue<I> queue = new ArrayBlockingQueue<I>(inputs.size());
    for (I input : inputs) {
      queue.add(input);
    }
    List<TaskCallable<I, O, T>> tasks = new ObjectArrayList<TaskCallable<I, O, T>>(
      numThreads);
    for (int i = 0; i < numThreads; i++) {
      TaskCallable<I, O, T> taskCallable = new TaskCallable<I, O, T>(queue,
        task, i);
      tasks.add(taskCallable);
    }
    // Execute
    Set<Future<Result<O>>> futureSet = new ObjectOpenHashSet<Future<Result<O>>>(
      numThreads);
    Set<Result<O>> rSet = new ObjectOpenHashSet<Result<O>>(numThreads);
    for (int i = 0; i < tasks.size(); i++) {
      Future<Result<O>> future = taskExecutor.submit(tasks.get(i));
      futureSet.add(future);
    }
    // Get result
    boolean success = true;
    for (Future<Result<O>> future : futureSet) {
      try {
        rSet.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error to collect output data.", e);
        success = false;
        break;
      }
    }
    if (!success) {
      return null;
    }
    List<O> output = new ObjectArrayList<O>(inputs.size());
    for (Result<O> result : rSet) {
      output.addAll(result.getDataList());
    }
    return output;
  }

  public static <I, O, T extends Task<I, O>> List<O> doTasks(I[] inputs,
    String taskName, T task, int numThreads) {
    if (inputs.length == 0) {
      return null;
    }
    if (numThreads == 0) {
      numThreads = Runtime.getRuntime().availableProcessors();
    }
    if (numThreads > inputs.length) {
      numThreads = inputs.length;
    }
    final BlockingQueue<I> queue = new ArrayBlockingQueue<I>(inputs.length);
    for (I input : inputs) {
      queue.add(input);
    }
    List<TaskCallable<I, O, T>> tasks = new ObjectArrayList<TaskCallable<I, O, T>>(
      numThreads);
    for (int i = 0; i < numThreads; i++) {
      TaskCallable<I, O, T> taskCallable = new TaskCallable<I, O, T>(queue,
        task, i);
      tasks.add(taskCallable);
    }
    // Execute
    Set<Future<Result<O>>> futureSet = new ObjectOpenHashSet<Future<Result<O>>>(
      numThreads);
    Set<Result<O>> rSet = new ObjectOpenHashSet<Result<O>>(numThreads);
    for (int i = 0; i < tasks.size(); i++) {
      Future<Result<O>> future = taskExecutor.submit(tasks.get(i));
      futureSet.add(future);
    }
    // Get result
    boolean success = true;
    for (Future<Result<O>> future : futureSet) {
      try {
        rSet.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error to collect output data.", e);
        success = false;
        break;
      }
    }
    if (!success) {
      return null;
    }
    List<O> output = new ObjectArrayList<O>(inputs.length);
    for (Result<O> result : rSet) {
      output.addAll(result.getDataList());
    }
    return output;
  }

  public static <I, O, T extends Task<I, O>> Set<O> doTasksReturnSet(
    List<I> inputs, String taskName, T task, int numThreads) {
    if (numThreads > inputs.size()) {
      numThreads = inputs.size();
    }
    if (numThreads == 0) {
      return new HashSet<O>();
    }
    final BlockingQueue<I> queue = new ArrayBlockingQueue<I>(inputs.size());
    for (I input : inputs) {
      queue.add(input);
    }
    List<TaskCallable<I, O, T>> tasks = new ObjectArrayList<TaskCallable<I, O, T>>();
    for (int i = 0; i < numThreads; i++) {
      TaskCallable<I, O, T> taskCallable = new TaskCallable<I, O, T>(queue,
        task, i);
      tasks.add(taskCallable);
    }
    // Execute
    Set<Future<Result<O>>> futureSet = new ObjectOpenHashSet<Future<Result<O>>>();
    Set<Result<O>> rSet = new ObjectOpenHashSet<Result<O>>(numThreads);
    for (int i = 0; i < tasks.size(); i++) {
      Future<Result<O>> future = taskExecutor.submit(tasks.get(i));
      futureSet.add(future);
    }
    // Get result
    boolean success = true;
    for (Future<Result<O>> future : futureSet) {
      try {
        rSet.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error to collect output data.", e);
        success = false;
        break;
      }
    }
    if (!success) {
      return null;
    }
    Set<O> output = new HashSet<O>();
    for (Result<O> result : rSet) {
      output.addAll(result.getDataList());
    }
    return output;
  }

  public static <I, O, T extends Task<I, O>> Set<O> doThreadTasks(
    List<I> inputs, String taskName, T task, int numThreads) {
    if (numThreads > inputs.size()) {
      numThreads = inputs.size();
    }
    final BlockingQueue<I> queue = new ArrayBlockingQueue<I>(inputs.size());
    for (I input : inputs) {
      queue.add(input);
    }
    List<TaskCallable<I, O, T>> tasks = new ObjectArrayList<TaskCallable<I, O, T>>();
    for (int i = 0; i < numThreads; i++) {
      TaskCallable<I, O, T> taskCallable = new TaskCallable<I, O, T>(queue,
        task, i);
      tasks.add(taskCallable);
    }
    // Execute
    Set<Future<Result<O>>> futureSet = new ObjectOpenHashSet<Future<Result<O>>>();
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < tasks.size(); i++) {
      RunnableFuture<Result<O>> future = new FutureTask<Result<O>>(tasks.get(i));
      Thread thread = new Thread(future);
      threads.add(thread);
      futureSet.add(future);
    }
    for (Thread thread : threads) {
      thread.start();
    }
    boolean success = true;
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOG.error("Fail to join a thread.", e);
        success = false;
        break;
      }
    }
    if (!success) {
      return null;
    }
    // Get results
    Set<Result<O>> rSet = new ObjectOpenHashSet<Result<O>>(numThreads);
    for (Future<Result<O>> future : futureSet) {
      try {
        rSet.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error to collect output data.", e);
        success = false;
        break;
      }
    }
    if (!success) {
      return null;
    }
    // Collect results
    Set<O> output = new HashSet<O>();
    for (Result<O> result : rSet) {
      output.addAll(result.getDataList());
    }
    return output;
  }
}
