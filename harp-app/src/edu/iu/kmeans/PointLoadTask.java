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

package edu.iu.kmeans;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.iu.harp.collective.Task;
import edu.iu.harp.comm.data.DoubleArray;

public class PointLoadTask extends Task<String, DoubleArray> {


  protected static final Log LOG = LogFactory.getLog(PointLoadTask.class);
  
  private int pointsPerFile;
  private int vectorSize;
  private Configuration conf;

  public PointLoadTask(int pPerFile, int vSize, Configuration conf) {
    this.pointsPerFile = pPerFile;
    this.vectorSize = vSize;
    this.conf = conf;
  }

  @Override
  public DoubleArray run(String fileName) throws Exception {
    int pointsSize = pointsPerFile * vectorSize;
    DoubleArray array = null;
    int count = 0;
    boolean success = false;
    do {
      try {
        array = loadPoints(fileName, pointsSize, conf);
        success = true;
      } catch (Exception e) {
        LOG.error("load " + fileName + " fails. Count=" + count, e);
        Thread.sleep(100);
        success = false;
        count++;
      }

    } while (!success && count < 100);
    if(count == 100) {
      throw new Exception("Fail to load files.");
    }
    return array;
  }

  /**
   * Load data points from a file.
   * 
   * @param file
   * @param conf
   * @return
   * @throws IOException
   */
  public static DoubleArray loadPoints(String file, int pointsSize,
    Configuration conf) throws Exception {
    double[] points = new double[pointsSize];
    Path pointFilePath = new Path(file);
    FileSystem fs = pointFilePath.getFileSystem(conf);
    FSDataInputStream in = fs.open(pointFilePath);
    try {
      for (int i = 0; i < pointsSize; i++) {
        points[i] = in.readDouble();
      }
    } finally {
      in.close();
    }
    DoubleArray pArray = new DoubleArray();
    pArray.setArray(points);
    pArray.setSize(pointsSize);
    return pArray;
  }
}
