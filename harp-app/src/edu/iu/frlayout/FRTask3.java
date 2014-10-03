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
import edu.iu.harp.collective.Task;
import edu.iu.harp.graph.vtx.IntFltArrVtxPartition;
import edu.iu.harp.graph.vtx.IntFltArrVtxTable;
import edu.iu.harp.util.Int2ObjectReuseHashMap;

public class FRTask3 extends Task<IntFltArrVtxPartition, IntFltArrVtxPartition> {

  private IntFltArrVtxTable allGraphLayout;
  private float t;

  public FRTask3(IntFltArrVtxTable graphLayout, double t) {
    allGraphLayout = graphLayout;
    this.t = (float) t;
  }

  @Override
  public IntFltArrVtxPartition run(IntFltArrVtxPartition sgPartition)
    throws Exception {
    float t = this.t;
    int sgVtxID;
    float[] sgDblArr = null;
    float[] glDblArr1 = null;
    float ded;
    Int2ObjectReuseHashMap<float[]> sgMap = sgPartition.getVertexMap();
    for (Int2ObjectMap.Entry<float[]> sgEntry : sgMap.int2ObjectEntrySet()) {
      sgVtxID = sgEntry.getIntKey();
      sgDblArr = sgEntry.getValue();
      ded = (float) Math.sqrt(sgDblArr[0] * sgDblArr[0] + sgDblArr[1]
        * sgDblArr[1]);
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
