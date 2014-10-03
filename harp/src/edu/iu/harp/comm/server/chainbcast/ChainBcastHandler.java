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

package edu.iu.harp.comm.server.chainbcast;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.CommUtil;
import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.WorkerInfo;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;

public abstract class ChainBcastHandler implements Runnable {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ChainBcastHandler.class);

  private final Connection conn;
  private final ResourcePool pool;
  private final WorkerData workerData;
  private final boolean isMax;
  private final WorkerInfo next;
  private final byte command;

  ChainBcastHandler(WorkerData workerData, ResourcePool pool, Connection conn,
    Workers workers, byte command) {
    this.workerData = workerData;
    this.pool = pool;
    this.conn = conn;
    this.isMax = workers.isMax();
    this.next = workers.getNextInfo();
    this.command = command;
  }

  @Override
  public void run() {
    Connection nextConn = null;
    if (!this.isMax) {
      nextConn = CommUtil.startConnection(this.next.getNode(),
        this.next.getPort());
      if (nextConn == null) {
        LOG.error("Fail to start next connection.");
        this.conn.close();
        return;
      }
      // LOG.info("Succeed to start next connection.");
    } else {
      // LOG.info("No next connection.");
    }
    Commutable data = null;
    try {
      data = receiveData(conn, nextConn);
    } catch (Exception e) {
      LOG.error("Error in receiving data.", e);
      this.conn.close();
      if (nextConn != null) {
        nextConn.close();
      }
    }
    // Put to the queue
    this.workerData.putCommData(data);
  }

  protected ResourcePool getResourcePool() {
    return this.pool;
  }

  protected byte getCommand() {
    return this.command;
  }

  protected abstract Commutable receiveData(Connection conn, Connection nextConn)
    throws Exception;
}
