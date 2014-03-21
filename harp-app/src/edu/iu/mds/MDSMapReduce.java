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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.iu.common.DataFileInputFormat;
import edu.iu.common.MultiFileInputFormat;

public class MDSMapReduce extends Configured implements Tool {

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MDSMapReduce(), argv);
    System.exit(res);
  }

  /**
   * Launches all the tasks in order.
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 8) {
      System.err
        .println("Usage: hadoop jar mds-hadoop.jar <input dir> <x file> <x width>"
          + " <num data points> <num iterations> <output dir> <num map tasks> <num partitions per worker>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    String inputDir = args[0];
    String xFile = args[1];
    int xWidth = Integer.parseInt(args[2]);
    int numOfDataPoints = Integer.parseInt(args[3]);
    int numIteration = Integer.parseInt(args[4]);
    String outputDir = args[5];
    int numMapTasks = Integer.parseInt(args[6]);
    int partitionPerWorker = Integer.parseInt(args[7]);
    boolean generateData = true;
    launch(numOfDataPoints, numIteration, inputDir, outputDir, xFile, xWidth,
      numMapTasks, partitionPerWorker, generateData);
    return 0;
  }

  double launch(int numPoints, int numIterations, String inputDir,
    String outputDir, String xFile, int xWidth, int numMapTasks,
    int partitionPerWorker, boolean generateData) throws IOException,
    URISyntaxException, InterruptedException, ExecutionException,
    ClassNotFoundException {
    Configuration configuration = getConf();
    FileSystem fs = FileSystem.get(configuration);
    // Generate data here
    if (generateData) {
      DataGen.generateXData(numPoints, xWidth, configuration, xFile, fs);
      int numPartitions = numMapTasks * partitionPerWorker;
      DataGen.generateDistanceMatrix(numPoints, numPartitions, "mds_indatatmp",
        fs, inputDir);
    }
    double error = runMDSMR(numIterations, inputDir, outputDir, xFile,
      configuration, numPoints, xWidth, numMapTasks, partitionPerWorker);
    return error;
  }

  private double runMDSMR(int numIterations, String inputDir, String outputDir,
    String xFile, Configuration configuration, int numPoints, int xWidth,
    int numMapTasks, int partitionPerWorker) throws IOException,
    URISyntaxException, InterruptedException, ClassNotFoundException {
    System.out.println("Starting Job");
    long startTime = System.currentTimeMillis();
    long iterStartTime;
    double error = 0;
    int jobCount = 0;
    int numJobs = 1;
    int iterationCount = numIterations / numJobs;
    boolean jobSuccess = true;
    do {
      iterStartTime = System.currentTimeMillis();
      Job bcCalcJob = prepareBCCalcJob(inputDir, xFile, outputDir,
        iterationCount, jobCount, configuration, numPoints, xWidth,
        numMapTasks, partitionPerWorker);
      jobSuccess = bcCalcJob.waitForCompletion(true);
      if (!jobSuccess) {
        System.out.println("MDS BCCalc Job failed. Job:" + jobCount);
        break;
      }
      System.out.println("| Job #" + jobCount + " Finished in "
        + (System.currentTimeMillis() - iterStartTime) / 1000.0 + " seconds |");
      jobCount++;
    } while ((jobCount < numJobs) && jobSuccess);
    System.out.println("Hadoop MDS Job Finished in "
      + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    System.out.println("Number of jobs = " + jobCount);
    return error;
  }

  private Job prepareBCCalcJob(String inputDir, String xFile,
    String outputDirPath, int iterationCount, int jobCount,
    Configuration configuration, int numPoints, int xWidth, int numMapTasks,
    int partitionPerWorker) throws IOException, URISyntaxException,
    InterruptedException, ClassNotFoundException {
    Job job = new Job(configuration, "map-collective-mds-bc" + jobCount);
    Configuration jobConfig = job.getConfiguration();
    Path outputDir = new Path(outputDirPath);
    FileInputFormat.setInputPaths(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);
    jobConfig.setInt(MDSConstants.ITERATION, iterationCount);
    jobConfig.setInt(MDSConstants.NUMPOINTS, numPoints);
    jobConfig.setInt(MDSConstants.XWIDTH, xWidth);
    jobConfig.set(MDSConstants.XFILE, xFile);
    jobConfig.setInt(MDSConstants.NUM_MAPS, numMapTasks);
    jobConfig.setInt(MDSConstants.PARTITION_PER_WORKER, partitionPerWorker);
    // input class to file-based class
    job.setInputFormatClass(MultiFileInputFormat.class);
    // job.setInputFormatClass(DataFileInputFormat.class);
    // job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setJarByClass(MDSMapReduce.class);
    job.setMapperClass(MDSAllgatherMultiThreadMapper.class);
    // When use MultiFileInputFormat, remember to set the number of map tasks
    org.apache.hadoop.mapred.JobConf jobConf = (JobConf) job.getConfiguration();
    jobConf.set("mapreduce.framework.name", "map-collective");
    jobConf.setNumMapTasks(numMapTasks);
    job.setNumReduceTasks(0);
    return job;
  }
}
