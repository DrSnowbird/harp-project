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

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.data.Commutable;

/**
 * We use a queue to maintain the data order.
 */
public class WorkerData {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(WorkerData.class);
  
  private LinkedBlockingQueue<Commutable> dataQueue;

  public WorkerData() {
    dataQueue = new LinkedBlockingQueue<Commutable>();
  }

  public Commutable waitAndGetCommData(long timeout) {
    try {
      return dataQueue.poll(timeout, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Error in waiting and getting data.", e);
    }
    return null;
  }

  public void putCommData(Commutable commData) {
    this.dataQueue.add(commData);
  }
  
  public void putAllCommData(List<Commutable> allCommdata) {
    dataQueue.addAll(allCommdata);
  }
}
