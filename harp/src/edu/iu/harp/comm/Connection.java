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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

public class Connection {

  private String node;
  private int port;
  private DataOutputStream dout;
  private DataInputStream din;
  private Socket socket;

  /**
   * Connection as a client
   * 
   * @param host
   * @param port
   * @param timeOutMs
   * @throws Exception
   */
  public Connection(String node, int port, int timeOutMs) throws Exception {
    this.node = node;
    this.port = port;
    try {
      InetAddress addr = InetAddress.getByName(node);
      SocketAddress sockaddr = new InetSocketAddress(addr, port);
      this.socket = new Socket();
      int timeoutMs = timeOutMs;
      this.socket.connect(sockaddr, timeoutMs);
      this.dout = new DataOutputStream(socket.getOutputStream());
      this.din = new DataInputStream(socket.getInputStream());
    } catch (Exception e) {
      this.close();
      throw e;
    }
  }

  /**
   * Connection as a server
   * 
   * @param dout
   * @param din
   * @param socket
   */
  public Connection(String node, int port, DataOutputStream dout,
    DataInputStream din, Socket socket) {
    this.node = node;
    this.port = port;
    this.socket = socket;
    this.din = din;
    this.dout = dout;
  }

  public String getNode() {
    return this.node;
  }

  public int getPort() {
    return this.port;
  }

  public DataOutputStream getDataOutputStream() {
    return this.dout;
  }

  public DataInputStream getDataInputDtream() {
    return this.din;
  }

  public void close() {
    try {
      if (dout != null) {
        dout.close();
      }
    } catch (IOException e) {
    }
    try {
      if (din != null) {
        din.close();
      }
    } catch (IOException e) {
    }
    try {
      if (socket != null) {
        socket.close();
      }
    } catch (IOException e) {
    }
    this.dout = null;
    this.din = null;
    this.socket = null;
  }
}
