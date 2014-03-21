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
import java.util.List;
import java.util.Set;

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
 * This mapper is modified from original mapper in Hadoop.
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

  private BufferedReader getNodesReader(int workerID, Context context)
    throws IOException {
    String jobDir = "/" + context.getJobID().toString();
    String nodseFile = jobDir + "/nodes";
    String lockFile = jobDir + "/lock";
    LOG.info("TRY LOCK FILE " + lockFile + "; Worker ID " + workerID);
    boolean retry = false;
    int retryCount = 0;
    do {
      try {
        Path path = new Path(lockFile);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        retry = !fs.exists(path);
      } catch (Exception e) {
        retry = true;
        retryCount++;
        LOG.error("Error when reading nodes lock file.", e);
        if (retryCount == Constants.RETRY_COUNT) {
          break;
        }
      }
    } while (retry);
    LOG.info("Get NODES FILE " + nodseFile + "; Worker ID " + workerID);
    Path path = new Path(nodseFile);
    FileSystem fs = FileSystem.get(context.getConfiguration());
    FSDataInputStream in = fs.open(path);
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    return br;
  }

  private boolean initCollCommComponents(Context context) throws IOException {
    // Get worker ID
    workerID = context.getTaskAttemptID().getTaskID().getId();
    LOG.info("WORKER SETUP: " + workerID);
    // Get nodes file and initialize workers
    BufferedReader br = getNodesReader(workerID, context);
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
    boolean success = CollCommWorker.masterBarrier(workers, workerData,
      resourcePool);
    LOG.info("Barrier: " + success);
    return success;
  }

  protected int getWorkerID() {
    return this.workerID;
  }
  
  protected boolean isMaster() {
    return this.workers.isMaster();
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
  
  public <I, O, T extends Task<I, O>> List<O> doTasks(List<I> inputs,
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
    } catch (Exception e) {
      LOG.error("Fail to do map-collective.", e);
      throw new IOException(e);
    } finally {
      cleanup(context);
      receiver.stop();
    }
  }
}