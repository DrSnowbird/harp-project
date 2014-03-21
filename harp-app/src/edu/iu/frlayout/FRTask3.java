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

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import edu.iu.harp.collective.Task;
import edu.iu.harp.graph.vtx.LongDblArrVtxPartition;
import edu.iu.harp.graph.vtx.LongDblArrVtxTable;
import edu.iu.harp.util.Long2ObjectOpenHashMap;

public class FRTask3 extends
  Task<LongDblArrVtxPartition, LongDblArrVtxPartition> {

  private LongDblArrVtxTable allGraphLayout;
  private double t;

  public FRTask3(LongDblArrVtxTable graphLayout, double t) {
    allGraphLayout = graphLayout;
    this.t = t;
  }

  @Override
  public LongDblArrVtxPartition run(LongDblArrVtxPartition sgPartition)
    throws Exception {
    long sgVtxID;
    double[] sgDblArr = null;
    double[] glDblArr1 = null;
    double ded;
    Long2ObjectOpenHashMap<double[]> sgMap = sgPartition.getVertexMap();
    for (Long2ObjectMap.Entry<double[]> sgEntry : sgMap.long2ObjectEntrySet()) {
      sgVtxID = sgEntry.getLongKey();
      sgDblArr = sgEntry.getValue();
      ded = Math.sqrt(sgDblArr[0] * sgDblArr[0] + sgDblArr[1] * sgDblArr[1]);
      if (ded > t) {
        ded = t / ded;
        sgDblArr[0] *= ded;
        sgDblArr[1] *= ded;
      }
      glDblArr1 = allGraphLayout.getVertexVal(sgVtxID);
      glDblArr1[0] += sgDblArr[0];
      glDblArr1[1] += sgDblArr[1];
    }
    return allGraphLayout.getPartition(sgPartition.getPartitionID());
  }
}
