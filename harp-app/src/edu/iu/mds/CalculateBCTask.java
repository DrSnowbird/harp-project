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

package edu.iu.mds;

import edu.iu.harp.arrpar.ArrPartition;
import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;
import edu.iu.harp.comm.resource.ResourcePool;

public class CalculateBCTask extends Task<Data, Data> {

  private ArrPartition<DoubleArray>[] xPartitions;
  private int[] xIndex;
  private int xWidth;
  private ResourcePool resourcePool;

  public CalculateBCTask(ArrPartition<DoubleArray>[] xPartitions, int[] xIndex,
    int xWidth, ResourcePool resourcePool) {
    this.xPartitions = xPartitions;
    this.xIndex = xIndex;
    this.xWidth = xWidth;
    this.resourcePool = resourcePool;
  }

  @Override
  public Data run(Data data) throws Exception {
    int bcSize = data.height * this.xWidth;
    double[] doubles = this.resourcePool.getDoubleArrayPool().getArray(bcSize);
    DoubleArray bcArray = new DoubleArray();
    bcArray.setArray(doubles);
    bcArray.setSize(bcSize);
    Data bcData = new Data();
    bcData.height = data.height;
    bcData.width = this.xWidth;
    bcData.row = data.row;
    bcData.rowOffset = data.rowOffset;
    bcData.array = bcArray;
    MDSAllgatherMultiThreadMapper.calculateBC(data.array, xPartitions, xIndex,
      xWidth, bcArray, data.rowOffset, data.height, data.width,
      this.resourcePool);
    return bcData;
  }
}
