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

package edu.iu.harp.comm.client.chainbcast;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.CommUtil;
import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.WorkerInfo;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;

public abstract class ChainBcastMaster {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ChainBcastMaster.class);

  private final Commutable data;
  private final boolean isMax;
  private final WorkerInfo nextWorker;
  private final ResourcePool resourcePool;
  private byte command;

  public ChainBcastMaster(Commutable data, Workers workers,
    ResourcePool resourcePool) throws Exception {
    // Make sure some settings before execution
    if (!workers.isMaster()) {
      throw new Exception();
    }
    this.data = data;
    this.nextWorker = workers.getNextInfo();
    this.resourcePool = resourcePool;
    this.isMax = workers.isMax();
  }

  public boolean execute() {
    // If this is the max worker (also master),
    // don't execute, return
    if (this.isMax) {
      return true;
    }
    Commutable processedData = null;
    try {
      processedData = processData(this.getData());
    } catch (Exception e) {
      LOG.error("Error in processing data.", e);
      return false;
    }
    Connection conn = CommUtil.startConnection(nextWorker.getNode(),
      nextWorker.getPort());
    if (conn == null) {
      releaseProcessedData(processedData);
      return false;
    }
    // Send
    boolean sendSuccess = true;
    try {
      sendProcessedData(conn, processedData);
    } catch (Exception e) {
      LOG.error("Error when broadcasting data.", e);
      sendSuccess = false;
    } finally {
      conn.close();
      releaseProcessedData(processedData);
    }
    return sendSuccess;
  }

  protected void setCommand(byte command) {
    this.command = command;
  }

  protected byte getCommand() {
    return this.command;
  }

  protected Commutable getData() {
    return this.data;
  }

  protected ResourcePool getResourcePool() {
    return this.resourcePool;
  }

  protected abstract Commutable processData(Commutable data) throws Exception;

  protected abstract void releaseProcessedData(Commutable data);

  protected abstract void sendProcessedData(Connection conn, Commutable data)
    throws Exception;
}
