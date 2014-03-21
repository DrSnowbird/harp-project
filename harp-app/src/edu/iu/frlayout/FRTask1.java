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

import java.util.Random;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import edu.iu.harp.collective.Task;
import edu.iu.harp.graph.vtx.LongDblArrVtxPartition;
import edu.iu.harp.graph.vtx.LongDblArrVtxTable;
import edu.iu.harp.util.Long2ObjectOpenHashMap;

class Null {
  public Null() {
  }
}

public class FRTask1 extends Task<LongDblArrVtxPartition, Null> {

  private LongDblArrVtxTable allGraphLayout;
  private Random r;
  private double ks;
  private double area;

  public FRTask1(LongDblArrVtxTable graphLayout, double ks, double area) {
    allGraphLayout = graphLayout;
    r = new Random();
    this.ks = ks;
    this.area = area;
  }

  @Override
  public Null run(LongDblArrVtxPartition sgPartition) throws Exception {
    long sgVtxID;
    double[] sgDblArr = null;
    long glVtxID;
    double[] glDblArr1 = null;
    double[] glDblArr2 = null;
    double xd;
    double yd;
    double dedS;
    double ded;
    double rf;
    Long2ObjectOpenHashMap<double[]> sgMap = sgPartition.getVertexMap();
    for (Long2ObjectMap.Entry<double[]> sgEntry : sgMap.long2ObjectEntrySet()) {
      sgVtxID = sgEntry.getLongKey();
      sgDblArr = sgEntry.getValue();
      for (LongDblArrVtxPartition glPartition : allGraphLayout.getPartitions()) {
        Long2ObjectOpenHashMap<double[]> glMap = glPartition.getVertexMap();
        for (Long2ObjectMap.Entry<double[]> glEntry : glMap
          .long2ObjectEntrySet()) {
          glVtxID = glEntry.getLongKey();
          glDblArr1 = glEntry.getValue();
          if (sgVtxID == glVtxID) {
            continue;
          }
          glDblArr2 = allGraphLayout.getVertexVal(sgVtxID);
          xd = glDblArr2[0] - glDblArr1[0];
          yd = glDblArr2[1] - glDblArr1[1];
          dedS = xd * xd + yd * yd;
          ded = Math.sqrt(dedS);
          rf = 0;
          if (ded != 0) {
            /*
             * xd /= ded; yd /= ded; rf = ks * (float)(1.0 / ded - dedS /
             * repulseRad); disps[m][0] += xd * rf; disps[m][1] += yd * rf;
             */
            rf = ks / dedS - ded / area;
            sgDblArr[0] += xd * rf;
            sgDblArr[1] += yd * rf;
          } else {
            xd = r.nextGaussian() * 0.1;
            yd = r.nextGaussian() * 0.1;
            rf = r.nextGaussian() * 0.1;
            sgDblArr[0] += xd * rf;
            sgDblArr[1] += yd * rf;
          }
        }
      }
    }
    return new Null();
  }
}
