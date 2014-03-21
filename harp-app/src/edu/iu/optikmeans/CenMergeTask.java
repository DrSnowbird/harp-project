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

package edu.iu.optikmeans;

import java.util.List;
import java.util.Map;

import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;

class NullCenMergeTaskOut {
  public NullCenMergeTaskOut() {
  }
}

public class CenMergeTask extends Task<Integer, NullCenMergeTaskOut> {
  private final List<Map<Integer, DoubleArray>> cenDataMaps;
  private final Map<Integer, DoubleArray> cenOutMap;

  public CenMergeTask(List<Map<Integer, DoubleArray>> cenDataMaps,
    Map<Integer, DoubleArray> cenOutMap) {
    this.cenDataMaps = cenDataMaps;
    this.cenOutMap = cenOutMap;
  }

  @Override
  public NullCenMergeTaskOut run(Integer partitionID) throws Exception {
    DoubleArray cenOutData = this.cenOutMap.get(partitionID);
    double[] cenOutDataArr = cenOutData.getArray();
    DoubleArray cenData = null;
    double[] cenDataArr = null;
    Map<Integer, DoubleArray> cenDataMap = null;
    for (int i = 0; i < this.cenDataMaps.size(); i++) {
      cenDataMap = this.cenDataMaps.get(i);
      cenData = cenDataMap.get(partitionID);
      cenDataArr = cenData.getArray();
      for (int j = 0; j < cenData.getSize(); j++) {
        cenOutDataArr[j] = cenOutDataArr[j] + cenDataArr[j];
      }
    }
    return new NullCenMergeTaskOut();
  }
}
