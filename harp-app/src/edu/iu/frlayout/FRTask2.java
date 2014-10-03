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

import org.apache.log4j.Logger;

import edu.iu.harp.collective.Task;
import edu.iu.harp.graph.EdgePartition;
import edu.iu.harp.graph.IntVertexID;
import edu.iu.harp.graph.NullEdgeVal;
import edu.iu.harp.graph.vtx.IntFltArrVtxTable;

public class FRTask2 extends
  Task<EdgePartition<IntVertexID, NullEdgeVal>, Null> {

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(FRTask2.class);

  private IntFltArrVtxTable allGraphLayout;
  private IntFltArrVtxTable sgDisps;
  private float k;

  public FRTask2(IntFltArrVtxTable graphLayout, IntFltArrVtxTable disps,
    double k, double area) {
    allGraphLayout = graphLayout;
    sgDisps = disps;
    this.k = (float) k;
  }

  @Override
  public Null run(EdgePartition<IntVertexID, NullEdgeVal> partition)
    throws Exception {
    float k = (float) this.k;
    float[] sgDblArr = null;
    float[] glDblArr1 = null;
    float[] glDblArr2 = null;
    float xd;
    float yd;
    float ded;
    float af;
    IntVertexID srcID = null;
    IntVertexID tgtID = null;
    Random r = new Random();
    while (partition.nextEdge()) {
      // Here is to reduce the number of times to fetch glDblArr1 from
      // allGraphLayout
      if (srcID == null) {
        srcID = partition.getCurSourceID();
        glDblArr1 = allGraphLayout.getVertexVal(srcID.getVertexID());
      } else if (srcID.getVertexID() != partition.getCurSourceID()
        .getVertexID()) {
        srcID = partition.getCurSourceID();
        glDblArr1 = allGraphLayout.getVertexVal(srcID.getVertexID());
      }
      tgtID = partition.getCurTargetID();
      glDblArr2 = allGraphLayout.getVertexVal(tgtID.getVertexID());
      if (glDblArr1 == null) {
        LOG.info("glDblArr1 is null " + srcID.getVertexID());
      }
      if (glDblArr2 == null) {
        LOG.info("glDblArr2 is null " + tgtID.getVertexID());
      }
      xd = glDblArr1[0] - glDblArr2[0];
      yd = glDblArr1[1] - glDblArr2[1];
      ded = (float) (Math.sqrt(xd * xd + yd * yd));
      af = 0;
      // The partition layout between edge partition and sgDisps
      // should be the same, no synchronization
      sgDblArr = sgDisps.getVertexVal(srcID.getVertexID());
      if (ded != 0) {
        af = ded / k;
        sgDblArr[0] -= xd * af;
        sgDblArr[1] -= yd * af;
      } else {
        xd = (float) (r.nextGaussian() * 0.1);
        yd = (float) (r.nextGaussian() * 0.1);
        af = (float) (r.nextGaussian() * 0.1);
        sgDblArr[0] -= xd * af;
        sgDblArr[1] -= yd * af;
      }
    }
    partition.defaultReadPos();
    return new Null();
  }
}
