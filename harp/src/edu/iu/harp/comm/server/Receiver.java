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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.CommUtil;
import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.Constants;
import edu.iu.harp.comm.WorkerData;
import edu.iu.harp.comm.Workers;
import edu.iu.harp.comm.request.ChainBcastAck;
import edu.iu.harp.comm.resource.ResourcePool;
import edu.iu.harp.comm.server.chainbcast.ByteArrChainBcastHandler;
import edu.iu.harp.comm.server.chainbcast.ChainBcastHandler;
import edu.iu.harp.comm.server.chainbcast.DoubleArrChainBcastHandler;
import edu.iu.harp.comm.server.chainbcast.IntArrChainBcastHandler;
import edu.iu.harp.comm.server.chainbcast.StructObjChainBcastHandler;
import edu.iu.harp.comm.server.chainbcast.WritableObjChainBcastHandler;

public class Receiver implements Runnable {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(Receiver.class);

  /** Drop box to send data back to the main program */
  private WorkerData workerData;
  /** Resource pool */
  private ResourcePool pool;
  /** Executors */
  private ExecutorService receiverExecutor;
  private ExecutorService handlerExecutor;
  /** Make sure the access is synchronized */
  private final Workers workers;
  /** Cache necessary information since "workers" is global */
  private final String node;
  private final int port;
  /** Server socket */
  private final ServerSocket serverSocket;

  public Receiver(WorkerData data, ResourcePool pool, Workers workers,
    String node, int port, int threads) throws Exception {
    this.workerData = data;
    this.pool = pool;
    receiverExecutor = Executors.newSingleThreadExecutor();
    handlerExecutor = Executors.newFixedThreadPool(threads);
    this.workers = workers;
    // Cache local information
    this.node = node;
    this.port = port;
    // Server socket
    try {
      serverSocket = new ServerSocket(this.port);
      serverSocket.setReuseAddress(true);
    } catch (Exception e) {
      LOG.error("Error in starting receiver.", e);
      throw new Exception(e);
    }
    LOG.info("Receiver starts on " + this.node + " " + this.port);
  }

  public void start() {
    receiverExecutor.execute(this);
  }

  public void stop() {
    CommUtil.closeReceiver(this.node, this.port);
    CommUtil.closeExecutor(receiverExecutor, "receiverExecutor");
    LOG.info("Receiver is stopped on " + this.node + " " + this.port);
  }

  @Override
  public void run() {
    while (true) {
      try {
        Socket socket = serverSocket.accept();
        // socket.setSendBufferSize(64 * 1024);
        // socket.setReceiveBufferSize(128 * 1024);
        // LOG.info("ReceiveBufferSize: " + socket.getReceiveBufferSize()
        // + " SendBufferSize: " + socket.getSendBufferSize());
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();
        // Receiver connection
        Connection conn = new Connection(this.node, this.port, out, in, socket);
        // All commands should use positive byte integer 0 ~ 127
        byte commType = (byte) in.read();
        if (commType == Constants.RECEIVER_QUIT_REQUEST) {
          conn.close();
          break;
        } else if (commType == Constants.WRITABLE_OBJ_CHAIN_BCAST) {
          ChainBcastHandler handler = new WritableObjChainBcastHandler(
            this.workerData, this.pool, conn, this.workers,
            Constants.WRITABLE_OBJ_CHAIN_BCAST);
          handlerExecutor.execute(handler);
        } else if (commType == Constants.STRUCT_OBJ_CHAIN_BCAST) {
          ChainBcastHandler handler = new StructObjChainBcastHandler(
            this.workerData, this.pool, conn, this.workers,
            Constants.STRUCT_OBJ_CHAIN_BCAST);
          handlerExecutor.execute(handler);
        } else if (commType == Constants.BYTE_ARRAY_CHAIN_BCAST) {
          ChainBcastHandler handler = new ByteArrChainBcastHandler(
            this.workerData, this.pool, conn, this.workers,
            Constants.BYTE_ARRAY_CHAIN_BCAST);
          handlerExecutor.execute(handler);
        } else if (commType == Constants.INT_ARRAY_CHAIN_BCAST) {
          ChainBcastHandler handler = new IntArrChainBcastHandler(
            this.workerData, this.pool, conn, this.workers,
            Constants.INT_ARRAY_CHAIN_BCAST);
          handlerExecutor.execute(handler);
        } else if (commType == Constants.DOUBLE_ARRAY_CHAIN_BCAST) {
          ChainBcastHandler handler = new DoubleArrChainBcastHandler(
            this.workerData, this.pool, conn, this.workers,
            Constants.DOUBLE_ARRAY_CHAIN_BCAST);
          handlerExecutor.execute(handler);
        } else if (commType == Constants.WRITABLE_OBJ_REQUEST) {
          ReqHandler handler = new WritableObjReqHandler(this.workerData,
            this.pool, conn);
          handlerExecutor.execute(handler);
        } else if (commType == Constants.STRUCT_OBJ_REQUEST) {
          ReqHandler handler = new StructObjReqHandler(this.workerData,
            this.pool, conn);
          handlerExecutor.execute(handler);
        } else if (commType == Constants.INT_ARRAY_REQUEST) {
          ReqHandler handler = new IntArrReqHandler(this.workerData, this.pool,
            conn);
          handlerExecutor.execute(handler);
        } else if (commType == Constants.BYTE_ARRAY_REQUEST) {
          ReqHandler handler = new ByteArrReqHandler(this.workerData,
            this.pool, conn);
          handlerExecutor.execute(handler);
        } else if (commType == Constants.CHAIN_BCAST_ACK) {
          ChainBcastAck ack = (ChainBcastAck) this.pool.getWritableObjectPool()
            .getWritableObject(ChainBcastAck.class.getName());
          ack.setAckID(ack.getAckID() + 1);
          this.workerData.putCommData(ack);
        } else {
          LOG.info("Unknown command: " + commType + ". " + this.node + " "
            + this.port);
        }
      } catch (IOException e) {
        LOG.error("Exception on Node " + this.node + " " + this.port, e);
      }
    }
    // Shutdown all handler tasks
    LOG.info("Start closing executors");
    CommUtil.closeExecutor(handlerExecutor, "Receiver Executor");
    // Close server socket
    try {
      serverSocket.close();
    } catch (IOException e) {
      LOG.error(e);
    }
    LOG.info("Finish closing executors");
    LOG.info("Receiver ends");
  }
}