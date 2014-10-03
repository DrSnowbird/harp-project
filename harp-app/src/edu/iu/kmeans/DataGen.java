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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DataGen {

  /**
   * Generate centroids and upload to the cDir
   * 
   * @param numCentroids
   * @param vectorSize
   * @param configuration
   * @param random
   * @param cDir
   * @param fs
   * @throws IOException
   */
  static void generateCentroids(int numCentroids, int vectorSize,
    Configuration configuration, Path cDir, FileSystem fs, int startJobID)
    throws IOException {
    Random random = new Random();
    double[] data = null;
    if (fs.exists(cDir))
      fs.delete(cDir, true);
    if (!fs.mkdirs(cDir)) {
      throw new IOException("Mkdirs failed to create " + cDir.toString());
    }
    data = new double[numCentroids * vectorSize];
    for (int i = 0; i < data.length; i++) {
      // data[i] = 1000;
      data[i] = random.nextDouble() * 1000;
    }
    Path initClustersFile = new Path(cDir, KMeansConstants.CENTROID_FILE_PREFIX
      + startJobID);
    System.out.println("Generate centroid data." + initClustersFile.toString());
    FSDataOutputStream out = fs.create(initClustersFile, true);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
    for (int i = 0; i < data.length; i++) {
      // out.writeDouble(data[i]);
      bw.write(data[i] + " ");
      if ((i % vectorSize) == (vectorSize - 1)) {
        bw.write(data[i] + "");
        bw.newLine();
      } else {
        bw.write(data[i] + " ");
      }
    }
    bw.flush();
    bw.close();
    // out.flush();
    // out.sync();
    // out.close();
    System.out.println("Wrote centroids data to file");
  }

  /**
   * Generate data and upload to the data dir.
   * 
   * @param numOfDataPoints
   * @param vectorSize
   * @param numPointFiles
   * @param localInputDir
   * @param fs
   * @param dataDir
   * @throws IOException
   * @throws InterruptedException
   * @throws ExecutionException
   */
  static void generateVectors(int numOfDataPoints, int vectorSize,
    int numPointFiles, String localInputDir, FileSystem fs, Path dataDir)
    throws IOException, InterruptedException, ExecutionException {
    int pointsPerFile = numOfDataPoints / numPointFiles;
    System.out.println("Writing " + pointsPerFile + " vectors to a file");
    // Check data directory
    if (fs.exists(dataDir)) {
      fs.delete(dataDir, true);
    }
    // Check local directory
    File localDir = new File(localInputDir);
    // If existed, regenerate data
    if (localDir.exists() && localDir.isDirectory()) {
      for (File file : localDir.listFiles()) {
        file.delete();
      }
      localDir.delete();
    }
    boolean success = localDir.mkdir();
    if (success) {
      System.out.println("Directory: " + localInputDir + " created");
    }
    if (pointsPerFile == 0) {
      throw new IOException("No point to write.");
    }
    // Create random data points
    int poolSize = Runtime.getRuntime().availableProcessors();
    ExecutorService service = Executors.newFixedThreadPool(poolSize);
    List<Future<?>> futures = new ArrayList<Future<?>>();
    for (int k = 0; k < numPointFiles; k++) {
      Future<?> f = service.submit(new DataGenRunnable(pointsPerFile,
        localInputDir, Integer.toString(k), vectorSize));
      futures.add(f); // add a new thread
    }
    for (Future<?> f : futures) {
      f.get();
    }
    // Shut down the executor service so that this thread can exit
    service.shutdownNow();
    // Wrap to path object
    Path localInput = new Path(localInputDir);
    fs.copyFromLocalFile(localInput, dataDir);
  }

  static void generateData(int numDataPoints, int numCentroids, int vectorSize,
    int numPointFiles, Configuration configuration, FileSystem fs,
    Path dataDir, Path cenDir, String localDir, int startJobID)
    throws IOException, InterruptedException, ExecutionException {
    System.out.println("Generating data..... ");
    generateVectors(numDataPoints, vectorSize, numPointFiles, localDir, fs,
      dataDir);
  }
}
