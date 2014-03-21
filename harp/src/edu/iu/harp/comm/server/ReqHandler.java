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

package edu.iu.harp.comm.server;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;

public abstract class ReqHandler implements Runnable {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ReqHandler.class);

  private final Connection conn;
  private final WorkerData workerData;
  private final ResourcePool pool;

  public ReqHandler(WorkerData workerData, ResourcePool pool,
    Connection conn) {
    this.workerData = workerData;
    this.pool = pool;
    this.conn = conn;
  }

  @Override
  public void run() {
    try {
      Commutable data = handleData(this.conn);
      // Put to the queue
      this.workerData.putCommData(data);
    } catch (Exception e) {
      this.conn.close();
      LOG.error("Exception in handling data", e);
    }
  }

  protected ResourcePool getResourcePool() {
    return this.pool;
  }

  abstract protected Commutable handleData(Connection conn) throws Exception;
}
