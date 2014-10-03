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
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.iu.common.MultiFileInputFormat;

public class KMeansMapCollective extends Configured implements Tool {

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new Configuration(), new KMeansMapCollective(),
      argv);
    System.exit(res);
  }

  /**
   * Launches all the tasks in order.
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 10) {
      System.err
        .println("Usage: edu.iu.kmeans.KMeansMapCollective <num Of DataPoints> <num of Centroids> <vector size> "
          + "<number of map tasks> <partition per worker> <number of iteration> <iteration per job> <start Job ID>"
          + "<work dir> <local points dir>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    int numOfDataPoints = Integer.parseInt(args[0]);
    int numCentroids = Integer.parseInt(args[1]);
    int vectorSize = Integer.parseInt(args[2]);
    int numMapTasks = Integer.parseInt(args[3]);
    int partitionPerWorker = Integer.parseInt(args[4]);
    int numIteration = Integer.parseInt(args[5]);
    int iterationPerJob = Integer.parseInt(args[6]);
    int startJobID = Integer.parseInt(args[7]);
    String workDir = args[8];
    String localPointFilesDir = args[9];
    boolean regenerateData = true;
    if (args.length == 11) {
      regenerateData = Boolean.parseBoolean(args[10]);
    }
    System.out.println("Number of Map Tasks = " + numMapTasks);
    int numPointFiles = numMapTasks * partitionPerWorker;
    if (numOfDataPoints / numPointFiles == 0 || numCentroids / numMapTasks == 0) {
      return -1;
    }
    if (numIteration == 0) {
      numIteration = 1;
    }
    if (iterationPerJob == 0) {
      iterationPerJob = 1;
    }
    launch(numOfDataPoints, numCentroids, vectorSize, numPointFiles,
      numMapTasks, numIteration, iterationPerJob, startJobID, workDir,
      localPointFilesDir, regenerateData);
    return 0;
  }

  private void launch(int numOfDataPoints, int numCentroids, int vectorSize,
    int numPointFiles, int numMapTasks, int numIterations, int iterationPerJob,
    int startJobID, String workDir, String localPointFilesDir,
    boolean generateData) throws IOException, URISyntaxException,
    InterruptedException, ExecutionException, ClassNotFoundException {
    Configuration configuration = getConf();
    Path workDirPath = new Path(workDir);
    FileSystem fs = FileSystem.get(configuration);
    Path dataDir = new Path(workDirPath, "data");
    Path cenDir = new Path(workDirPath, "centroids");
    Path outDir = new Path(workDirPath, "out");
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    fs.mkdirs(outDir);
    if (generateData) {
      System.out.println("Generate data.");
      DataGen.generateData(numOfDataPoints, numCentroids, vectorSize,
        numPointFiles, configuration, fs, dataDir, cenDir, localPointFilesDir,
        startJobID);
    }
    DataGen.generateCentroids(numCentroids, vectorSize, configuration, cenDir, fs,
      startJobID);
    long startTime = System.currentTimeMillis();
    runKMeansAllReduce(numOfDataPoints, numCentroids, vectorSize,
      numIterations, iterationPerJob, startJobID, numPointFiles, numMapTasks,
      configuration, workDirPath, dataDir, cenDir, outDir);
    long endTime = System.currentTimeMillis();
    System.out
      .println("Total K-means Execution Time: " + (endTime - startTime));
  }

  private void runKMeansAllReduce(int numOfDataPoints, int numCentroids,
    int vectorSize, int numIterations, int iterationPerJob, int startJobID,
    int numPointFiles, int numMapTasks, Configuration configuration,
    Path workDirPath, Path dataDir, Path cDir, Path outDir) throws IOException,
    URISyntaxException, InterruptedException, ClassNotFoundException {
    System.out.println("Starting Job");
    int initialStartJobID = startJobID;
    long perJobSubmitTime;
    int iterationCount = 0;
    boolean jobSuccess = true;
    int jobRetryCount = 0;
    do {
      // ----------------------------------------------------------------------
      perJobSubmitTime = System.currentTimeMillis();
      System.out.println("Start Job#"
        + startJobID
        + " "
        + new SimpleDateFormat("HH:mm:ss.SSS").format(Calendar.getInstance()
          .getTime()));
      Job kmeansJob = configureKMeansJob(numOfDataPoints, numCentroids,
        vectorSize, numPointFiles, numMapTasks, configuration, workDirPath,
        dataDir, cDir, outDir, startJobID, iterationPerJob);
      System.out.println("| Job#" + startJobID + " configure in "
        + (System.currentTimeMillis() - perJobSubmitTime) + " miliseconds |");
      // -----------------------------------------------------------------------
      jobSuccess = kmeansJob.waitForCompletion(true);
      System.out.println("end Jod#"
        + startJobID
        + " "
        + new SimpleDateFormat("HH:mm:ss.SSS").format(Calendar.getInstance()
          .getTime()));
      System.out.println("| Job#" + startJobID + " Finished in "
        + (System.currentTimeMillis() - perJobSubmitTime) + " miliseconds |");
      // -----------------------------------------------------------------------
      if (!jobSuccess) {
        System.out.println("KMeans Job failed. Job ID:" + startJobID);
        jobRetryCount++;
        if (jobRetryCount == 3) {
          break;
        }
      } else {
        iterationCount += iterationPerJob;
        if ((numIterations - iterationCount) < iterationPerJob) {
          iterationPerJob = numIterations - iterationCount;
        }
        startJobID++;
        jobRetryCount = 0;
      }
    } while (iterationCount < numIterations);
    System.out.println("Number of jobs = " + (startJobID - initialStartJobID)
      + ", number of iterations = " + numIterations);
  }

  private Job configureKMeansJob(int numOfDataPoints, int numCentroids,
    int vectorSize, int numPointFiles, int numMapTasks,
    Configuration configuration, Path workDirPath, Path dataDir, Path cDir,
    Path outDir, int jobID, int iterationCount) throws IOException,
    URISyntaxException {
    Job job = new Job(configuration, "kmeans_job_" + jobID);
    Configuration jobConfig = job.getConfiguration();
    Path jobOutDir = new Path(outDir, "kmeans_out_" + jobID);
    FileSystem fs = FileSystem.get(configuration);
    if (fs.exists(jobOutDir)) {
      fs.delete(jobOutDir, true);
    }
    FileInputFormat.setInputPaths(job, dataDir);
    FileOutputFormat.setOutputPath(job, jobOutDir);
    // The first centroid file with ID 0,
    // which should match with the centroid file name in data generation
    Path cFile = new Path(cDir, KMeansConstants.CENTROID_FILE_PREFIX + jobID);
    System.out.println("Centroid File Path: " + cFile.toString());
    jobConfig.set(KMeansConstants.CFILE, cFile.toString());
    jobConfig.setInt(KMeansConstants.JOB_ID, jobID);
    jobConfig.setInt(KMeansConstants.ITERATION_COUNT, iterationCount);
    // input class to file-based class
    // job.setInputFormatClass(DataFileInputFormat.class);
    job.setInputFormatClass(MultiFileInputFormat.class);
    // job.setOutputKeyClass(IntWritable.class);
    // job.setOutputValueClass(V2DDataWritable.class);
    // job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setJarByClass(KMeansMapCollective.class);
    job.setMapperClass(KMeansCollectiveMapper.class);
    org.apache.hadoop.mapred.JobConf jobConf = (JobConf) job.getConfiguration();
    jobConf.set("mapreduce.framework.name", "map-collective");
    jobConf.setNumMapTasks(numMapTasks);
    jobConf.setInt("mapreduce.job.max.split.locations", 10000);
    job.setNumReduceTasks(0);
    jobConfig.setInt(KMeansConstants.VECTOR_SIZE, vectorSize);
    jobConfig.setInt(KMeansConstants.NUM_CENTROIDS, numCentroids);
    jobConfig.setInt(KMeansConstants.POINTS_PER_FILE, numOfDataPoints
      / numPointFiles);
    jobConfig.set(KMeansConstants.WORK_DIR, workDirPath.toString());
    jobConfig.setInt(KMeansConstants.NUM_MAPPERS, numMapTasks);
    return job;
  }
}
