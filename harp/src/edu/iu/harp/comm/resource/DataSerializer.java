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

import java.io.DataOutput;
import java.io.IOException;

public class DataSerializer implements DataOutput {

  private byte[] bytes;
  private int pos;

  public DataSerializer(byte[] bytes) {
    this.bytes = bytes;
    this.pos = 0;
  }

  public DataSerializer(byte[] bytes, int pos) {
    this.bytes = bytes;
    this.pos = pos;
  }
  
  public void setData(byte[] bytes) {
    this.bytes = bytes;
    this.pos = 0;
  }

  @Override
  public void write(int b) throws IOException {
    b = b & 0xFF;
    writeByte(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    System.arraycopy(b, 0, bytes, pos, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    System.arraycopy(b, off, bytes, pos, len);
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
    if (v) {
      bytes[pos] = 1;
    } else {
      bytes[pos] = 0;
    }
    pos++;
  }

  @Override
  public void writeByte(int v) throws IOException {
    bytes[pos] = (byte) ((v >>> 0) & 0xFF);
    pos++;
  }

  @Override
  public void writeShort(int v) throws IOException {
    bytes[pos] = (byte) ((v >>> 8) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 0) & 0xFF);
    pos++;
  }

  @Override
  public void writeChar(int v) throws IOException {
    bytes[pos] = (byte) ((v >>> 8) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 0) & 0xFF);
    pos++;
  }

  @Override
  public void writeInt(int v) throws IOException {
    bytes[pos] = (byte) ((v >>> 24) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 16) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 8) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 0) & 0xFF);
    pos++;
  }

  @Override
  public void writeLong(long v) throws IOException {
    bytes[pos] = (byte) ((v >>> 56) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 48) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 40) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 32) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 24) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 16) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 8) & 0xFF);
    pos++;
    bytes[pos] = (byte) ((v >>> 0) & 0xFF);
    pos++;
  }

  @Override
  public void writeFloat(float v) throws IOException {
    int vI = Float.floatToIntBits(v);
    writeInt(vI);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    long vL = Double.doubleToLongBits(v);
    writeLong(vL);
  }

  @Override
  public void writeBytes(String s) throws IOException {
    writeChars(s);
  }

  @Override
  public void writeChars(String s) throws IOException {
    int len = s.length();
    writeInt(len);
    for (int i = 0; i < len; i++) {
      writeChar(s.charAt(i));
    }
  }

  @Override
  public void writeUTF(String s) throws IOException {
    writeChars(s);
  }
}
