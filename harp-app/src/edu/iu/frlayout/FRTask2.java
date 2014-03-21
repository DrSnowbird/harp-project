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
import edu.iu.harp.graph.LongVertexID;
import edu.iu.harp.graph.NullEdgeVal;
import edu.iu.harp.graph.vtx.LongDblArrVtxTable;

public class FRTask2 extends
  Task<EdgePartition<LongVertexID, NullEdgeVal>, Null> {

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(FRTask2.class);
  
  private LongDblArrVtxTable allGraphLayout;
  private LongDblArrVtxTable sgDisps;
  private Random r;
  private double k;

  public FRTask2(LongDblArrVtxTable graphLayout, LongDblArrVtxTable disps,
    double k, double area) {
    allGraphLayout = graphLayout;
    sgDisps = disps;
    r = new Random();
    this.k = k;
  }

  @Override
  public Null run(EdgePartition<LongVertexID, NullEdgeVal> partition)
    throws Exception {
    double[] sgDblArr = null;
    double[] glDblArr1 = null;
    double[] glDblArr2 = null;
    double xd;
    double yd;
    double ded;
    double af;
    LongVertexID srcID = null;
    LongVertexID tgtID = null;
    while (partition.nextEdge()) {
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
      ded = Math.sqrt(xd * xd + yd * yd);
      af = 0;
      // The partition layout between edge partition and sgDisps
      // should be the same, no synchronization
      sgDblArr = sgDisps.getVertexVal(srcID.getVertexID());
      if (ded != 0) {
        /*
         * xd /= ded; yd /= ded; af = ded * ded / k; disps[m][0] -= xd * af;
         * disps[m][1] -= yd * af;
         */
        af = ded / k;
        sgDblArr[0] -= xd * af;
        sgDblArr[1] -= yd * af;
      } else {
        xd = r.nextGaussian() * 0.1;
        yd = r.nextGaussian() * 0.1;
        af = r.nextGaussian() * 0.1;
        sgDblArr[0] -= xd * af;
        sgDblArr[1] -= yd * af;
      }
    }
    partition.defaultReadPos();
    return new Null();
  }
}
