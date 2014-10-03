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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.CommUtil;
import edu.iu.harp.comm.Connection;
import edu.iu.harp.comm.Constants;

/**
 * In real cases, tasks or configurations bacsted to all workers needs separate
 * reply for workers.
 * 
 * But in bcast benchmark, we may need a slave to send reply to master directly.
 * 
 * @author zhangbj
 * 
 */
public class ChainBcastSlave {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ChainBcastSlave.class);

  public static void sendACK(String host, int port) {
    Connection conn = CommUtil.startConnection(host, port);
    OutputStream out = conn.getOutputStream();
    try {
      out.write(Constants.CHAIN_BCAST_ACK);
    } catch (IOException e) {
      LOG.error("Fail to send bcast ACK.", e);
    }
    conn.close();
  }
}
