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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Constants;

public class ReqSendCenter {
  /** Class logger */
  protected static final Logger LOG = Logger.getLogger(ReqSendCenter.class);

  private static final ExecutorService taskExecutor = Executors
    .newFixedThreadPool(Constants.NUM_SENDER_THREADS);
  private static final Set<Future<SendResult>> futureSet = new ObjectOpenHashSet<Future<SendResult>>(
    Constants.NUM_SENDER_THREADS);

  public static void submit(ReqSender sender) {

    Future<SendResult> future = taskExecutor.submit(new SendThread(sender));
    futureSet.add(future);
  }

  public static void waitForFinish() {
    for (Future<SendResult> future : futureSet) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error to collect SendResult.", e);
      }
    }
    futureSet.clear();
  }
}

class SendThread implements Callable<SendResult> {
  private ReqSender sender;

  SendThread(ReqSender sender) {
    this.sender = sender;
  }

  @Override
  public SendResult call() throws Exception {
    sender.execute();
    return new SendResult();
  }
}

class SendResult {
}