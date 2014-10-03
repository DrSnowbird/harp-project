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

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.data.Commutable;

public class CommUtil {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(CommUtil.class);

  public static Connection startConnection(String host, int port) {
    Connection conn = null;
    boolean isException = false;
    int count = 0;
    do {
      isException = false;
      try {
        conn = new Connection(host, port, 0);
      } catch (Exception e) {
        // Try to solve
        // java.net.NoRouteToHostException: Cannot assign requested address
        if (e instanceof java.net.NoRouteToHostException) {
          LOG.error("NoRouteToHostException when connecting " + host + ":"
            + port, e);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
          }
          isException = true;
          count++;
        } else if (e instanceof java.net.ConnectException) {
          LOG.error("Exception when connecting " + host + ":" + port + ". "
            + e.getMessage());
        } else {
          LOG.error("Exception when connecting " + host + ":" + port, e);
        }
      }
    } while (isException && count < 60);
    if (isException) {
      LOG.error("Fail to connect " + host + ":" + port);
    }
    return conn;
  }

  public static void closeReceiver(String ip, int port) {
    Connection conn = null;
    try {
      // close the receiver on this node
      conn = new Connection(ip, port, 0);
    } catch (Exception e) {
      e.printStackTrace();
      conn = null;
    }
    if (conn == null) {
      return;
    }
    try {
      OutputStream out = conn.getOutputStream();
      out.write(Constants.RECEIVER_QUIT_REQUEST);
      out.flush();
      conn.close();
    } catch (Exception e) {
      conn.close();
    }
  }

  public static void closeExecutor(ExecutorService executor,
    String executorName, long maxtime) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(maxtime, TimeUnit.SECONDS)) {
        LOG
          .info(executorName + " still works after " + maxtime + " seconds...");
        executor.shutdownNow();
        if (!executor.awaitTermination(Constants.TERMINATION_TIMEOUT_2,
          TimeUnit.SECONDS)) {
          LOG.info(executorName + " did not terminate with "
            + Constants.TERMINATION_TIMEOUT_2 + " more.");
        }
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public static void closeExecutor(ExecutorService executor, String executorName) {
    closeExecutor(executor, executorName, Constants.TERMINATION_TIMEOUT_1);
  }

  /**
   * Wait either maxWaitTime or maxWaitCount comes first
   * 
   * @param workerData
   * @param cClass
   * @param maxTimeOut
   * @param maxWaitCount
   * @return
   */
  public static <C extends Commutable> C waitAndGet(WorkerData workerData,
    Class<C> cClass, long maxTimeOut, int maxWaitCount) {
    Commutable data = null;
    int curWaitCount = 0;
    List<Commutable> waitList = new ObjectArrayList<Commutable>(maxWaitCount);
    do {
      data = workerData.waitAndGetCommData(maxTimeOut);
      if (data == null) {
        LOG.error("MAX TIME OUT. NO DATA.");
        break;
      } else if (!data.getClass().equals(cClass)) {
        // LOG.error("IRRELEVANT DATA TYPE: " + data.getClass().getName());
        waitList.add(data);
        curWaitCount++;
        data = null;
      }
    } while (data == null && (curWaitCount < maxWaitCount));
    if (waitList.size() > 0) {
      workerData.putAllCommData(waitList);
    }
    if (data == null) {
      return null;
    }
    // Make sure data is C type and then convert
    return (C) data;
  }
}
