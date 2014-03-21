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

package edu.iu.harp.graph.vtx;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.CommUtil;
import edu.iu.harp.comm.client.ReqSender;

public class ReqSendCommander {
  /** Class logger */
  protected static final Logger LOG = Logger.getLogger(ReqSendCommander.class);

  private int maxSendCmd;
  private int numThreads;
  private ExecutorService taskExecutor;
  private Set<Future<SendResult>> futureSet;
  private Set<SendResult> resultSet;

  public ReqSendCommander(int maxSendCmd, int numThreads) {
    this.maxSendCmd = maxSendCmd;
    this.numThreads = numThreads;
    this.taskExecutor = null;
    this.futureSet = null;
    this.resultSet = null;
  }

  public void start() {
    this.taskExecutor = Executors.newFixedThreadPool(this.numThreads);
    this.futureSet = new ObjectOpenHashSet<Future<SendResult>>(this.maxSendCmd);
    this.resultSet = new ObjectOpenHashSet<SendResult>(this.maxSendCmd);
  }

  public void addSender(ReqSender sender) {
    LOG.info("Schedule Sending struct object");
    Future<SendResult> future = taskExecutor.submit(new SendThread(sender));
    futureSet.add(future);
  }

  public void close() {
    for (Future<SendResult> future : futureSet) {
      try {
        resultSet.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error to collect SendResult.", e);
      }
    }
    CommUtil.closeExecutor(this.taskExecutor, "StructObjReqSendCommander");
  }

  private class SendThread implements Callable<SendResult> {
    private ReqSender sender;

    SendThread(ReqSender sender) {
      this.sender = sender;
    }

    @Override
    public SendResult call() throws Exception {
      SendResult result = new SendResult();
      sender.execute();
      return result;
    }
  }

  private class SendResult {

    SendResult() {
    }
  }
}
