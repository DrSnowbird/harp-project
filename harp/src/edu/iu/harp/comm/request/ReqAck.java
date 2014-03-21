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

package edu.iu.harp.comm.request;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.comm.data.StructObject;

public class ReqAck extends StructObject {
  private int workerID;
  private int ackID;

  public ReqAck() {
  }

  public ReqAck(int workerID, int ackID) {
    this.workerID = workerID;
    this.ackID = ackID;
  }

  public int getWorkerID() {
    return this.workerID;
  }

  public void setWorkerID(int workerID) {
    this.workerID = workerID;
  }

  public void setAckID(int ackID) {
    this.ackID = ackID;
  }

  public int getAckID() {
    return this.ackID;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(workerID);
    out.writeInt(ackID);
  }

  @Override
  public void read(DataInput in) throws IOException {
    this.workerID = in.readInt();
    this.ackID = in.readInt();
  }

  @Override
  public int getSizeInBytes() {
    return 8;
  }

}
