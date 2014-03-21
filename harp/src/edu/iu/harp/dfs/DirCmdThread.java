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

package edu.iu.harp.dfs;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.iu.harp.depl.CMDOutput;
import edu.iu.harp.depl.QuickDeployment;

public class DirCmdThread implements Runnable {
  private final BlockingQueue<String> hostnameQueue;
  private final String action;
  private final String dir;
  private final AtomicBoolean error; 

  public DirCmdThread(BlockingQueue<String> hostnameQueue, String action,
    String dir, AtomicBoolean error) {
    this.hostnameQueue = hostnameQueue;
    this.action = action;
    this.dir = dir;
    this.error = error;
  }

  @Override
  public void run() {
    while (!hostnameQueue.isEmpty()) {
      String hostname = hostnameQueue.poll();
      if (hostname == null) {
        break;
      }
      String[] cmd = {"ssh", hostname, action, dir};
      CMDOutput output = QuickDeployment.executeCMDandForward(cmd);
      if(!output.getExecutionStatus()) {
        this.error.set(true);
      } 
    }
  }

}
