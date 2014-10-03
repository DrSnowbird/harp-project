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

package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import edu.iu.harp.arrpar.ArrCombiner;
import edu.iu.harp.arrpar.ArrConverter;
import edu.iu.harp.arrpar.ArrTable;
import edu.iu.harp.collective.AllgatherWorker;
import edu.iu.harp.collective.CollCommWorker;
import edu.iu.harp.collective.GraphWorker;
import edu.iu.harp.collective.GroupByWorker;
import edu.iu.harp.collective.RegroupWorker;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.data.Array;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.Receiver;
import edu.iu.harp.graph.EdgeTable;
import edu.iu.harp.graph.EdgeVal;
import edu.iu.harp.graph.MsgTable;
import edu.iu.harp.graph.MsgVal;
import edu.iu.harp.graph.VertexID;
import edu.iu.harp.graph.vtx.StructPartition;
import edu.iu.harp.graph.vtx.StructTable;
import edu.iu.harp.keyval.Key;
import edu.iu.harp.keyval.KeyValTable;
import edu.iu.harp.keyval.ValCombiner;
import edu.iu.harp.keyval.Value;

/**
 * CollectiveMapper is extended from original mapper in Hadoop. It includes new
 * APIs for in-memory collective communication.
 * 
 * @author zhangbj
 * 
 * @param <KEYIN> Input key
 * @param <VALUEIN> Input value
 * @param <KEYOUT> Output key
 * @param <VALUEOUT> Output value
 */
public class CollectiveMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  protected static final Log LOG = LogFactory.getLog(CollectiveMapper.class);

  private int workerID;
  private Workers workers;
  private ResourcePool resourcePool;
  private WorkerData workerData;
  private Receiver receiver;

  /**
   * A Key-Value reader to read key-value inputs for this worker.
   *
   * @author zhangbj
   */
  protected class KeyValReader {
    private Context context;

    protected KeyValReader(Context context) {
      this.context = context;
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
      return this.context.nextKeyValue();
    }

    public KEYIN getCurrentKey() throws IOException, InterruptedException {
      return this.context.getCurrentKey();
    }

    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
      return this.context.getCurrentValue();
    }
  }
  
  private boolean tryLockFile(String lockFile, FileSystem fs) {
    LOG.info("TRY LOCK FILE " + lockFile);
    boolean retry = false;
    int retryCount = 0;
    do {
      try {
        Path path = new Path(lockFile);
        retry = !fs.exists(path);
      } catch (Exception e) {
        retry = true;
        retryCount++;
        LOG.error("Error when reading nodes lock file.", e);
        if (retryCount == Constants.RETRY_COUNT) {
          return false;
        }
      }
    } while (retry);
    return true;
  }
  
  private Map<Integer, Integer> getTaskWorkerMap(String tasksFile, FileSystem fs) {
    LOG.info("Get task file " + tasksFile);
    Map<Integer, Integer> taskWorkerMap = null;
    Path path = new Path(tasksFile);
    try {
      taskWorkerMap = new TreeMap<Integer, Integer>();
      FSDataInputStream in = fs.open(path);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line = null;
      String[] tokens = null;
      while ((line = br.readLine()) != null) {
        tokens = line.split("\t");
        taskWorkerMap.put(Integer.parseInt(tokens[0]),
          Integer.parseInt(tokens[1]));
      }
      br.close();
    } catch (IOException e) {
      LOG.error("No TASK FILE FOUND");
      taskWorkerMap = null;
    }
    return taskWorkerMap;
  }

  private BufferedReader getNodesReader(String nodesFile,  FileSystem fs)
    throws IOException {
    LOG.info("Get nodes file " + nodesFile);
    Path path = new Path(nodesFile);
    FSDataInputStream in = fs.open(path);
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    return br;
  }

  private boolean initCollCommComponents(Context context) throws IOException {
    // Get file names
    String jobDir = "/" + context.getJobID().toString();
    String nodesFile = jobDir + "/nodes";
    String tasksFile = jobDir + "/tasks";
    String lockFile = jobDir + "/lock";
    FileSystem fs = FileSystem.get(context.getConfiguration());
    // Try lock
    boolean success = tryLockFile(lockFile, fs);
    if (!success) {
      return false;
    }
    Map<Integer, Integer> taskWorkerMap = getTaskWorkerMap(tasksFile, fs);
    // Get worker ID
    int taskID = context.getTaskAttemptID().getTaskID().getId();
    LOG.info("Task ID " + taskID);
    if (taskWorkerMap == null) {
      workerID = taskID;
    } else {
      workerID = taskWorkerMap.get(taskID);
    }
    LOG.info("WORKER ID: " + workerID);
    // Get nodes file and initialize workers
    BufferedReader br = getNodesReader(nodesFile, fs);
    try {
      workers = new Workers(br, workerID);
      br.close();
    } catch (Exception e) {
      LOG.error("Cannot initialize workers.", e);
      throw new IOException(e);
    }
    workerData = new WorkerData();
    resourcePool = new ResourcePool();
    // Initialize receiver
    String host = workers.getSelfInfo().getNode();
    int port = workers.getSelfInfo().getPort();
    try {
      receiver = new Receiver(workerData, resourcePool, workers, host, port,
        Constants.NUM_HANDLER_THREADS);
    } catch (Exception e) {
      LOG.error("Cannot initialize receivers.", e);
      throw new IOException(e);
    }
    receiver.start();
    context.getProgress();
    success = CollCommWorker.masterHandshake(workers, workerData,
      resourcePool);
    LOG.info("Barrier: " + success);
    return success;
  }

  /**
   * Get the ID of this worker.
   * 
   * @return Worker ID
   */
  protected int getWorkerID() {
    return this.workerID;
  }
  
  protected boolean isMaster() {
    return this.workers.isMaster();
  }
  
  protected boolean masterBarrier() {
    return CollCommWorker.masterBarrier(workers, workerData, resourcePool);
  }
  
  protected <T, A extends Array<T>> boolean prmtvArrBcast(A array) {
    return CollCommWorker.prmtvArrChainBcast(array, this.workers,
      this.workerData, this.resourcePool);
  }
  
  protected <T, A extends Array<T>, C extends ArrCombiner<A>> boolean arrTableBcast(
    ArrTable<A, C> arrTable) {
    return CollCommWorker.arrTableBcast(arrTable, this.workers,
      this.workerData, this.resourcePool);
  }
  
  protected <T, A extends Array<T>, C extends ArrCombiner<A>> boolean arrTableBcastTotalKnown(
    ArrTable<A, C> arrTable, int numTotalPartitions) {
    return CollCommWorker.arrTableBcastTotalKnown(arrTable, numTotalPartitions,
      this.workers, this.workerData, this.resourcePool);
  }
  
  protected <P extends StructPartition, T extends StructTable<P>> boolean structTableBcast(
    StructTable<P> structTable) {
    return CollCommWorker.structTableBcast(structTable, this.workers,
      this.workerData, this.resourcePool);
  }

  protected <K extends Key, V extends Value, C extends ValCombiner<V>> void groupbyCombine(
    KeyValTable<K, V, C> table) {
    GroupByWorker.groupbyCombine(workers, workerData, resourcePool, table);
  }

  protected <A1 extends Array<?>, C1 extends ArrCombiner<A1>,
    A2 extends Array<?>, C2 extends ArrCombiner<A2>, C extends ArrConverter<A1, A2>>
    void allreduce(
    ArrTable<A1, C1> oldTable, ArrTable<A2, C2> newTable, C converter)
    throws Exception {
    RegroupWorker.allreduce(workers, workerData, resourcePool, oldTable,
      newTable, converter);
  }
  
  protected <A extends Array<?>, C extends ArrCombiner<A>> void allgather(
    ArrTable<A, C> table) {
    AllgatherWorker.allgather(workers, workerData, resourcePool, table);
  }

  protected <A extends Array<?>, C extends ArrCombiner<A>> void allgatherOne(
    ArrTable<A, C> table) throws Exception {
    AllgatherWorker.allgatherOne(workers, workerData, resourcePool, table);
  }

  protected <A extends Array<?>, C extends ArrCombiner<A>> void allgatherTotalKnown(
    ArrTable<A, C> table, int totalParRecv) throws Exception {
    AllgatherWorker.allgatherTotalKnown(workers, workerData, resourcePool,
      table, totalParRecv);
  }
  
  protected <P extends StructPartition, T extends StructTable<P>, 
    I extends VertexID, M extends MsgVal> 
    void allMsgToAllVtx(MsgTable<I, M> msgTable, T vtxTable) throws Exception {
    GraphWorker.allMsgToAllVtx(msgTable, vtxTable, workers, workerData,
      resourcePool);
  }
  
  public <I extends VertexID, E extends EdgeVal, ET extends EdgeTable<I, E>, 
    VP extends StructPartition, VT extends StructTable<VP>> void allEdgeToAllVtx(
    ET edgeTable, VT vtxTable) throws Exception {
    GraphWorker.allEdgeToAllVtx(edgeTable, vtxTable, workers, workerData,
      resourcePool);
  }
  
  public <I extends VertexID, E extends EdgeVal, ET extends EdgeTable<I, E>> void regroupEdges(
    ET edgeTable) throws Exception {
    GraphWorker.regroupEdges(edgeTable, workers, workerData, resourcePool);
  }
  
  public <P extends StructPartition, T extends StructTable<P>> void allgatherVtx(
    T table) throws IOException {
    GraphWorker.allgatherVtx(workers, workerData, resourcePool, table);
  }
  
  public <P extends StructPartition, T extends StructTable<P>> void allgatherVtxTotalKnown(
    T table, int totalNumPartitions) throws IOException {
    GraphWorker.allgatherVtxTotalKnown(workers, workerData, resourcePool,
      table, totalNumPartitions);
  }
  
  public <I, O, T extends Task<I, O>> List<O> doTasks(List<I> inputs,
    String taskName, T task, int numThreads) {
    return CollCommWorker.doTasks(inputs, taskName, task, numThreads);
  }
  
  public <I, O, T extends Task<I, O>> List<O> doTasks(I[] inputs,
    String taskName, T task, int numThreads) {
    return CollCommWorker.doTasks(inputs, taskName, task, numThreads);
  }
  
  public <I, O, T extends Task<I, O>> Set<O> doTasksReturnSet(List<I> inputs,
    String taskName, T task, int numThreads) {
    return CollCommWorker.doTasksReturnSet(inputs, taskName, task, numThreads);
  }
  
  public <I, O, T extends Task<I, O>> Set<O> doThreadTasks(List<I> inputs,
    String taskName, T task, int numThreads) {
    return CollCommWorker.doThreadTasks(inputs, taskName, task, numThreads);
  }

  protected ResourcePool getResourcePool() {
    return this.resourcePool;
  }
  
  protected void logMemUsage() {
    LOG.info("Total Memory (bytes): " + " "
      + Runtime.getRuntime().totalMemory() + ", Free Memory (bytes): "
      + Runtime.getRuntime().freeMemory());
  }

  protected void logGCTime() {
    long totalGarbageCollections = 0;
    long garbageCollectionTime = 0;
    for (GarbageCollectorMXBean gc : ManagementFactory
      .getGarbageCollectorMXBeans()) {
      long count = gc.getCollectionCount();
      if (count >= 0) {
        totalGarbageCollections += count;
      }
      long time = gc.getCollectionTime();
      if (time >= 0) {
        garbageCollectionTime += time;
      }
    }
    LOG.info("Total Garbage Collections: " + totalGarbageCollections
      + ", Total Garbage Collection Time (ms): " + garbageCollectionTime);
  }

  /**
   * Called once at the beginning of the task.
   */
  protected void setup(Context context) throws IOException,
    InterruptedException {
    // NOTHING
  }

  protected void mapCollective(KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    while (reader.nextKeyValue()) {
      // Do...
    }
  }

  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context) throws IOException,
    InterruptedException {
    // NOTHING
  }

  /**
   * Expert users can override this method for more complete control over the
   * execution of the Mapper.
   * 
   * @param context
   * @throws IOException
   */
  public void run(Context context) throws IOException, InterruptedException {
    long time1 = System.currentTimeMillis();
    boolean success = initCollCommComponents(context);
    long time2 = System.currentTimeMillis();
    LOG.info("Initialize Collective Communication components (ms): "
      + (time2 - time1));
    if (!success) {
      receiver.stop();
      throw new IOException("Fail to do master barrier.");
    }
    setup(context);
    KeyValReader reader = new KeyValReader(context);
    try {
      mapCollective(reader, context);
    } catch (Throwable t) {
      LOG.error("Fail to do map-collective.", t);
      throw new IOException(t);
    } finally {
      cleanup(context);
      receiver.stop();
    }
  }
}