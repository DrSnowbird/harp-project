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

package edu.iu.harp.test;

import java.io.IOException;

import edu.iu.harp.comm.resource.DataDeserializer;
import edu.iu.harp.comm.resource.DataSerializer;

public class SerializeTest {

  public static void main(String args[]) throws IOException {
    int[] a = { 4, 501, 502 };
    char[] ac = { 'a', 'b', 'c' };
    String s = "dfsdfdfskjfl;skf;slfs32409";
    boolean[] ab = { true, false, true };
    short[] as = { 1001, 2003, 4005 };
    long[] al = { 90239, 43980, 43822938};
    double[] ad = {50000.454, 7845239.58, 24239.23423023};
    byte[] b = new byte[a.length * 4 + ac.length * 2 + + s.length() *2 + 4 + ab.length * 1
      + as.length * 2 + al.length * 8 + ad.length * 8 + 2 + 1];
    DataSerializer ds = new DataSerializer(b);
    for(int i = 0; i < a.length; i++) {
      ds.writeInt(a[i]);
    }
    for(int i = 0; i < ac.length; i++) {
      ds.writeChar(ac[i]);
    }
    ds.writeChars(s);
    for(int i = 0; i < ab.length; i++) {
      ds.writeBoolean(ab[i]);
    }
    for(int i = 0; i < as.length; i++) {
      ds.writeShort(as[i]);
    }
    for(int i = 0; i < al.length; i++) {
      ds.writeLong(al[i]);
    }
    for(int i = 0; i < ad.length; i++) {
      ds.writeDouble(ad[i]);
    } 
    ds.writeShort(-2);
    ds.writeByte(-2);
    DataDeserializer dd = new DataDeserializer(b);
    for (int i = 0; i < a.length; i++) {
      System.out.println(dd.readInt());
    }
    for (int i = 0; i < ac.length; i++) {
      System.out.println(dd.readChar());
    }
    System.out.println(dd.readUTF());
    for (int i = 0; i < ab.length; i++) {
      System.out.println(dd.readBoolean());
    }
    for (int i = 0; i < as.length; i++) {
      System.out.println(dd.readShort());
    }
    for (int i = 0; i < al.length; i++) {
      System.out.println(dd.readLong());
    }
    for (int i = 0; i < ad.length; i++) {
      System.out.println(dd.readDouble());
    }
    System.out.println(dd.readUnsignedShort());
    System.out.println(dd.readUnsignedByte());
  }
}
