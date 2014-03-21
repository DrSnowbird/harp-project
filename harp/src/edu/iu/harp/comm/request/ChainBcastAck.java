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

import edu.iu.harp.comm.data.WritableObject;

public class ChainBcastAck extends WritableObject {
  private int ackID;

  public ChainBcastAck() {    
  }
  
  public void setAckID(int id) {
    this.ackID = id;
  }
  
  public int getAckID() {
    return this.ackID;
  }

  @Override
  public void write(DataOutput out) throws IOException {  
    out.writeInt(ackID);
  }

  @Override
  public void read(DataInput in) throws IOException {
    this.ackID = in.readInt();
  }
}
