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

package edu.iu.wdamds;

import java.util.Arrays;

import org.apache.log4j.Logger;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class MMCalcTask extends Task<RowData, XRowData> {

  /** Class logger */
  protected static final Logger LOG = Logger.getLogger(MMCalcTask.class);

  private final ArrPartition<DoubleArray>[] xPartitions;
  private final int d;
  private final ResourcePool resourcePool;

  public MMCalcTask(ArrPartition<DoubleArray>[] xPartitions, int d,
    ResourcePool resourcePool) {
    this.xPartitions = xPartitions;
    this.d = d;
    this.resourcePool = resourcePool;
  }

  @Override
  public XRowData run(RowData rowData) throws Exception {
    // Copy to local
    int d = this.d;
    
    int mmSize = rowData.height * d;
    double[] mms = this.resourcePool.getDoubleArrayPool().getArray(mmSize);
    // mms array should be small, hope this won't generate
    // large overhead.
    for (int i = 0; i < mmSize; i++) {
      if (mms[i] != 0) {
        mms[i] = 0;
      }
    }
    DoubleArray mmArray = new DoubleArray();
    mmArray.setArray(mms);
    mmArray.setSize(mmSize);
    XRowData mmOutData = new XRowData();
    mmOutData.height = rowData.height;
    mmOutData.width = d;
    mmOutData.row = rowData.row;
    mmOutData.rowOffset = rowData.rowOffset;
    mmOutData.array = mmArray;
    // CalcUtil.matrixMultiply(rowData.vArray, xPartitions, xRowIndex, mmArray,
    //  rowData.height, d, rowData.width);
    CalcUtil.matrixMultiply(rowData.vArray, xPartitions, mmArray,
      rowData.height, d, rowData.width);
    return mmOutData;
  }
}
