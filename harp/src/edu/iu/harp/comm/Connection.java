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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

public class Connection {

  private String node;
  private int port;
  private OutputStream out;
  private InputStream in;
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
      this.out = socket.getOutputStream();
      this.in = socket.getInputStream();
    } catch (Exception e) {
      this.close();
      throw e;
    }
  }

  /**
   * Connection as a server
   * 
   * @param out
   * @param in
   * @param socket
   */
  public Connection(String node, int port, OutputStream out, InputStream in,
    Socket socket) {
    this.node = node;
    this.port = port;
    this.socket = socket;
    this.in = in;
    this.out = out;
  }

  public String getNode() {
    return this.node;
  }

  public int getPort() {
    return this.port;
  }

  public OutputStream getOutputStream() {
    return this.out;
  }

  public InputStream getInputDtream() {
    return this.in;
  }

  public void close() {
    try {
      if (out != null) {
        out.close();
      }
    } catch (IOException e) {
    }
    try {
      if (in != null) {
        in.close();
      }
    } catch (IOException e) {
    }
    try {
      if (socket != null) {
        socket.close();
      }
    } catch (IOException e) {
    }
    this.out = null;
    this.in = null;
    this.socket = null;
  }
}
