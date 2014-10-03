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

package edu.iu.frlayout;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Random;

import edu.iu.harp.collective.Task;
import edu.iu.harp.graph.vtx.IntFltArrVtxPartition;
import edu.iu.harp.graph.vtx.IntFltArrVtxTable;
import edu.iu.harp.util.Int2ObjectReuseHashMap;

class Null {
  public Null() {
  }
}

public class FRTask1 extends Task<IntFltArrVtxPartition, Null> {

  private IntFltArrVtxTable allGraphLayout;
  private IntFltArrVtxPartition[] glPartitions;
  private double ks;
  private double area;

  public FRTask1(IntFltArrVtxTable allGraphLayout,
    IntFltArrVtxPartition[] glPartitions, double ks, double area) {
    this.allGraphLayout = allGraphLayout;
    this.glPartitions = glPartitions;
    this.ks = ks;
    this.area = area;
  }

  @Override
  public Null run(IntFltArrVtxPartition sgPartition) throws Exception {
    float ks = (float) this.ks;
    float area = (float) this.area;
    IntFltArrVtxPartition[] glPartitions = this.glPartitions;
    int sgVtxID;
    float[] sgDblArr = null;
    int glVtxID;
    float[] glDblArr1 = null;
    float[] glDblArr2 = null;
    float xd;
    float yd;
    float dedS;
    float ded;
    float rf;
    float sgArr0 = 0;
    float sgArr1 = 0;
    float glArr0 = 0;
    float glArr1 = 0;
    // long outStart = 0;
    // long outEnd = 0;
    // long outTotal = 0;
    Random r = new Random();
    // sg iterator
    ObjectIterator<Int2ObjectMap.Entry<float[]>> sgMapEntryIterator = sgPartition
      .getVertexMap().int2ObjectEntrySet().fastIterator();
    Int2ObjectMap.Entry<float[]> sgMapEntry = null;
    // sg related graph layout map
    Int2ObjectReuseHashMap<float[]> sgGLMap = allGraphLayout.getPartition(
      sgPartition.getPartitionID()).getVertexMap();
    // All graph layout iterator
    ObjectIterator<Int2ObjectMap.Entry<float[]>> glMapEntryIterator = null;
    Int2ObjectMap.Entry<float[]> glMapEntry = null;
    while (sgMapEntryIterator.hasNext()) {
      sgMapEntry = sgMapEntryIterator.next();
      sgVtxID = sgMapEntry.getIntKey();
      sgDblArr = sgMapEntry.getValue();
      sgArr0 = sgDblArr[0];
      sgArr1 = sgDblArr[1];
      glDblArr2 = sgGLMap.get(sgVtxID);
      glArr0 = glDblArr2[0];
      glArr1 = glDblArr2[1];
      // outStart = System.nanoTime();
      for (int i = 0; i < glPartitions.length; i++) {
        glMapEntryIterator = glPartitions[i].getVertexMap()
          .int2ObjectEntrySet().fastIterator();
        while (glMapEntryIterator.hasNext()) {
          glMapEntry = glMapEntryIterator.next();
          glVtxID = glMapEntry.getIntKey();
          glDblArr1 = glMapEntry.getValue();
          if (sgVtxID == glVtxID) {
            continue;
          }
          // xd = (float) glDblArr2[0] - (float) glDblArr1[0];
          // yd = (float) glDblArr2[1] - (float) glDblArr1[1];
          xd = glArr0 - glDblArr1[0];
          yd = glArr1 - glDblArr1[1];
          dedS = xd * xd + yd * yd;
          ded = (float) Math.sqrt(dedS);
          if (ded != 0) {
            rf = ks / dedS - ded / area;
            // sgDblArr[0] += xd * rf;
            // sgDblArr[1] += yd * rf;
            sgArr0 += xd * rf;
            sgArr1 += yd * rf;
          } else {
            xd = (float) (r.nextGaussian() * 0.1);
            yd = (float) (r.nextGaussian() * 0.1);
            rf = (float) (r.nextGaussian() * 0.1);
            // sgDblArr[0] += xd * rf;
            // sgDblArr[1] += yd * rf;
            sgArr0 += xd * rf;
            sgArr1 += yd * rf;
          }
        }
      }
      // outEnd = System.nanoTime();
      // outTotal += (outEnd - outStart);
      sgDblArr[0] = sgArr0;
      sgDblArr[1] = sgArr1;
    }
    // System.out.println("Total core time (ms): "
    // + outTotal);
    return new Null();
  }
}
