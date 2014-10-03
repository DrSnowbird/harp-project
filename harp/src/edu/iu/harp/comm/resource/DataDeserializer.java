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

package edu.iu.harp.comm.resource;

import java.io.DataInput;
import java.io.IOException;

public class DataDeserializer implements DataInput {

  private byte[] bytes;
  private int pos;

  public DataDeserializer(byte[] bytes) {
    this.bytes = bytes;
    this.pos = 0;
  }

  public DataDeserializer(byte[] bytes, int pos) {
    this.bytes = bytes;
    this.pos = pos;
  }
  
  public void setData(byte[] bytes) {
    this.bytes = bytes;
    this.pos = 0;
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    System.arraycopy(bytes, pos, b, 0, b.length);
    pos = pos + b.length;
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    System.arraycopy(bytes, pos, b, off, len);
    pos = pos + len;
  }

  @Override
  public int skipBytes(int n) throws IOException {
    pos = pos + n;
    return pos;
  }

  @Override
  public boolean readBoolean() throws IOException {
    byte ch = bytes[pos];
    pos++;
    if (ch == 1) {
      return true;
    }
    return false;
  }

  @Override
  public byte readByte() throws IOException {
    byte ch = bytes[pos];
    pos++;
    return ch;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    int i = readByte();
    i = i & 0xFF;
    return i;
  }

  @Override
  public short readShort() throws IOException {
    short ch1 = bytes[pos];
    pos++;
    short ch2 = bytes[pos];
    pos++;
    return (short) (((ch1 & 0xFF) << 8) + ((ch2 & 0xFF) << 0));
  }

  @Override
  public int readUnsignedShort() throws IOException {
    int s = readShort();
    s = s & 0xFFFF;
    return s;
  }

  @Override
  public char readChar() throws IOException {
    char ch1 = (char) bytes[pos];
    pos++;
    char ch2 = (char) bytes[pos];
    pos++;
    return (char) (((ch1 & 0xFF) << 8) + ((ch2 & 0xFF) << 0));
  }

  @Override
  public int readInt() throws IOException {
    int ch1 = bytes[pos];
    pos++;
    int ch2 = bytes[pos];
    pos++;
    int ch3 = bytes[pos];
    pos++;
    int ch4 = bytes[pos];
    pos++;
    return (((ch1 & 0xFF) << 24) + ((ch2 & 0xFF) << 16) + ((ch3 & 0xFF) << 8) + ((ch4 & 0xFF) << 0));
  }

  @Override
  public long readLong() throws IOException {
    long ch1 = bytes[pos];
    pos++;
    long ch2 = bytes[pos];
    pos++;
    long ch3 = bytes[pos];
    pos++;
    long ch4 = bytes[pos];
    pos++;
    long ch5 = bytes[pos];
    pos++;
    long ch6 = bytes[pos];
    pos++;
    long ch7 = bytes[pos];
    pos++;
    long ch8 = bytes[pos];
    pos++;
    return (((ch1 & 0xFF) << 56) + ((ch2 & 0xFF) << 48) + ((ch3 & 0xFF) << 40)
      + ((ch4 & 0xFF) << 32) + ((ch5 & 0xFF) << 24) + ((ch6 & 0xFF) << 16)
      + ((ch7 & 0xFF) << 8) + ((ch8 & 0xFF) << 0));
  }

  @Override
  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  @Override
  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  @Override
  public String readLine() throws IOException {
    return readUTF();
  }

  @Override
  public String readUTF() throws IOException {
    int len = readInt();
    StringBuffer sb = new StringBuffer(len);
    for (int i = 0; i < len; i++) {
      sb.append(readChar());
    }
    return sb.toString();
  }
}
