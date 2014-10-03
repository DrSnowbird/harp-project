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

package edu.iu.harp.comm.client;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.CommUtil;
import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.data.Commutable;
import edu.iu.harp.comm.resource.ResourcePool;

/**
 * Currently we build the logic based on the simple logic. No fault tolerance is
 * considered.
 * 
 */
public abstract class ReqSender {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ReqSender.class);
  private final String host;
  private final int port;
  private Commutable data;
  private final ResourcePool resourcePool;
  private byte command;

  public ReqSender(String host, int port, Commutable data, ResourcePool pool) {
    this.host = host;
    this.port = port;
    this.data = data;
    this.resourcePool = pool;
  }

  public boolean execute() {
    // Process data
    // This is a local object
    Commutable processedData = null;
    try {
      processedData = processData(this.getData());
    } catch (Exception e) {
      LOG.error("Error in processing data.", e);
      return false;
    }
    Connection conn = CommUtil.startConnection(host, port);
    if (conn == null) {
      releaseProcessedData(processedData);
      return false;
    }
    // Send
    boolean sendSuccess = true;
    try {
      sendProcessedData(conn, processedData);
    } catch (Exception e) {
      LOG.error("Error in sending processed data.", e);
      sendSuccess = false;
    } finally {
      conn.close();
      releaseProcessedData(processedData);
    }
    return sendSuccess;
  }

  protected Commutable getData() {
    return this.data;
  }

  protected ResourcePool getResourcePool() {
    return this.resourcePool;
  }

  protected void setCommand(byte command) {
    this.command = command;
  }

  protected byte getCommand() {
    return this.command;
  }

  protected abstract Commutable processData(Commutable data) throws Exception;
  
  protected abstract void releaseProcessedData(Commutable data);

  protected abstract void sendProcessedData(Connection conn, Commutable data)
    throws Exception;
}
