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

package edu.iu.harp.comm;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.BufferedReader;
import java.util.Iterator;

import edu.iu.harp.depl.Nodes;

/**
 * Workers: 0... Self, Next, ... Max Master/Slave Master: 0 Slave: else
 * 
 * Not modifiable from outside
 */
public class Workers extends Nodes {

  /** Map between worker id and worker info */
  private final Int2ObjectOpenHashMap<WorkerInfo> workers;
  /** Map between rack id and worker id */
  private final Int2ObjectOpenHashMap<IntArrayList> rackWorkers;
  /** Worker ID of the current worker */
  private final int selfID;
  /** Master info (communication coordinator) */
  private final int masterID;
  private final WorkerInfo masterInfo;
  /** Max worker ID */
  private final int maxID;
  /** Worker ID of the next worker */
  private final int nextID;

  public Workers() throws Exception {
    // Get workers,
    // but self is not a member of workers
    // Then next is pointed to Worker 0 (master)
    this(-1);
  }

  public Workers(int selfID) throws Exception {
    this(null, selfID);
  }

  public Workers(BufferedReader reader, int selfID) throws Exception {
    super(reader);
    int workerPortBase = Constants.DEFAULT_WORKER_POART_BASE;
    workers = new Int2ObjectOpenHashMap<WorkerInfo>();
    rackWorkers = new Int2ObjectOpenHashMap<IntArrayList>();
    Int2ObjectOpenHashMap<ObjectArrayList<String>> nodes = this.getNodes();
    int workerID = -1;
    // Load based on the order in node file.
    for (int rackID : getRackList()) {
      IntArrayList workerIDs = new IntArrayList();
      rackWorkers.put(rackID, workerIDs);
      for (String node : nodes.get(rackID)) {
        // Generate next worker ID
        workerID++;
        // Port: workerPortBase + workerID
        workers.put(workerID, new WorkerInfo(workerID, node, workerPortBase
          + workerID, rackID));
        workerIDs.add(workerID);
      }
    }
    this.selfID = selfID;
    masterID = 0;
    masterInfo = workers.get(masterID);
    maxID = workerID;
    // Set next worker ID
    if (selfID >= 0 && selfID < maxID) {
      nextID = selfID + 1;
    } else {
      nextID = 0;
    }
  }
  
  public int getNumWorkers() {
    return workers.size();
  }

  public int getMasterID() {
    return this.masterID;
  }

  public boolean isMaster() {
    return selfID == masterID;
  }

  public WorkerInfo getMasterInfo() {
    return masterInfo;
  }

  public int getSelfID() {
    return selfID;
  }

  public boolean isSelfInWorker() {
    if (selfID >= 0 && selfID <= maxID) {
      return true;
    } else {
      return false;
    }
  }

  public WorkerInfo getSelfInfo() {
    return workers.get(selfID);
  }

  public boolean isMax() {
    return selfID == maxID;
  }

  public int getMaxID() {
    return maxID;
  }

  public int getNextID() {
    return nextID;
  }

  public WorkerInfo getNextInfo() {
    return workers.get(nextID);
  }

  public WorkerInfo getWorkerInfo(int workerID) {
    return workers.get(workerID);
  }

  public WorkerInfoList getWorkerInfoList() {
    return new WorkerInfoList();
  }

  public class WorkerInfoIterator implements Iterator<WorkerInfo> {
    protected int workerID = -1;

    @Override
    public boolean hasNext() {
      if ((workerID + 1) <= getMaxID()) {
        return true;
      }
      return false;
    }

    @Override
    public WorkerInfo next() {
      workerID = workerID + 1;
      return workers.get(workerID);
    }

    @Override
    public void remove() {
    }
  }

  public class WorkerInfoList implements Iterable<WorkerInfo> {

    @Override
    public Iterator<WorkerInfo> iterator() {
      return new WorkerInfoIterator();
    }
  }
}
