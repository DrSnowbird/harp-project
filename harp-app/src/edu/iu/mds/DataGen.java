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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DataGen {

  static void generateXData(int numPoints, int xWidth,
    Configuration configuration, String xFile, FileSystem fs)
    throws IOException {
    Path xFilePath = new Path(xFile);
    double[] data = null;
    if (fs.exists(xFilePath)) {
      fs.delete(xFilePath, true);
    }
    data = new double[numPoints * xWidth];
    for (int i = 0; i < data.length; i++) {
      data[i] = 1;
    }
    System.out.println("Generate X data: " + xFilePath.toString());
    FSDataOutputStream out = fs.create(xFilePath, true);
    for (int i = 0; i < data.length; i++) {
      out.writeDouble(data[i]);
    }
    out.flush();
    out.sync();
    out.close();
    System.out.println("Wrote X data to file");
  }

  static void generateDistanceMatrix(int numOfDataPoints, int numPartitions,
    String localDir, FileSystem fs, String dataDir) throws IOException,
    InterruptedException, ExecutionException {
    Path dataDirPath = new Path(dataDir);
    // Check data directory
    if (fs.exists(dataDirPath)) {
      fs.delete(dataDirPath, true);
    }
    // Check local directory
    File newDir = new File(localDir);
    // If existed, regenerate data
    if (newDir.exists()) {
      newDir.delete();
    }
    boolean success = newDir.mkdir();
    if (success) {
      System.out.println("Directory: " + localDir + " created");
    }
    // Create random data points
    // distMat[i][j] == distMat[j][i], distMat[i][i] = 0
    // We set all distance to 1
    double[] distMat = new double[numOfDataPoints * numOfDataPoints];
    for (int i = 0; i < numOfDataPoints; i++) {
      for (int j = 0; j < i; j++) {
        distMat[i * numOfDataPoints + j] = 1;
        distMat[j * numOfDataPoints + i] = 1;
      }
      distMat[i * numOfDataPoints + i] = 0;
    }
    int height = numOfDataPoints / numPartitions;
    int rest = numOfDataPoints % numPartitions;
    int rowStart = 0;
    int poolSize = Runtime.getRuntime().availableProcessors();
    ExecutorService service = Executors.newFixedThreadPool(poolSize);
    List<Future<?>> futures = new ArrayList<Future<?>>();
    for (int i = 0; i < numPartitions; i++) {
      Future<?> f = null;
      if (rest > 0) {
        rest--;
        // Width, height, row ID, row Offset
        f = service.submit(new DataGenRunnable(distMat, localDir,
          numOfDataPoints, height + 1, i, rowStart));
        rowStart = rowStart + height + 1;
      } else if (height > 0) {
        f = service.submit(new DataGenRunnable(distMat, localDir,
          numOfDataPoints, height, i, rowStart));
        rowStart = rowStart + height;
      } else {
        break;
      }
      futures.add(f); // add a new thread
    }
    for (Future<?> f : futures) {
      f.get();
    }
    // Shut down the executor service so that this thread can exit
    service.shutdownNow();
    // Wrap to path object
    Path localInput = new Path(localDir);
    fs.copyFromLocalFile(localInput, dataDirPath);
  }
}
