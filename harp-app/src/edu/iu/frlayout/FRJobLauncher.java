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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.iu.common.MultiFileInputFormat;

public class FRJobLauncher extends Configured implements Tool {

  /**
   * Launches all the tasks in order.
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 7) {
      System.err
        .println("Usage: hadoop jar hadoop-collecitve.jar <input dir> <layout file> "
          + "<total vtx> <num iterations> <num map tasks> <partiiton per worker> <output dir>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    String inputDir = args[0];
    String layoutFile = args[1];
    int totalVtx = Integer.parseInt(args[2]);
    int numIteration = Integer.parseInt(args[3]);
    int numMapTasks = Integer.parseInt(args[4]);
    int partitionPerWorker = Integer.parseInt(args[5]);
    String outputDir = args[6];
    launch(inputDir, layoutFile, totalVtx, numIteration, numMapTasks,
      partitionPerWorker, outputDir);
    return 0;
  }

  void launch(String inputDir, String layoutFile, int totalVtx,
    int numIterations, int numMapTasks, int partitionPerWorker, String outputDir)
    throws IOException, URISyntaxException, InterruptedException,
    ExecutionException, ClassNotFoundException {
    Configuration configuration = getConf();
    runFR(inputDir, layoutFile, totalVtx, numIterations, numMapTasks, partitionPerWorker,
      outputDir, configuration);
  }

  private void runFR(String inputDir, String layoutFile, int totalVtx,
    int numIterations, int numMapTasks, int partitionPerWorker, String outputDir,
    Configuration configuration) throws IOException, URISyntaxException,
    InterruptedException, ClassNotFoundException {
    long startTime = System.currentTimeMillis();
    long iterStartTime;
    int jobCount = 0;
    int numJobs = 1;
    int iterationCount = numIterations / numJobs;
    boolean jobSuccess = true;
    System.out.println("Starting FR Layout Job");
    do {
      iterStartTime = System.currentTimeMillis();
      Job job = prepareJob(inputDir, layoutFile, totalVtx, iterationCount,
        jobCount, numMapTasks, partitionPerWorker, outputDir, configuration);
      jobSuccess = job.waitForCompletion(true);
      if (!jobSuccess) {
        System.out.println("FR Layout Job failed. Job:" + jobCount);
        break;
      }
      System.out.println("| Job #" + jobCount + " Finished in "
        + (System.currentTimeMillis() - iterStartTime) / 1000.0 + " seconds |");
      jobCount++;
    } while ((jobCount < numJobs) && jobSuccess);
    System.out.println("Hadoop FR Layout Job Finished in "
      + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    System.out.println("Number of jobs = " + jobCount);
  }

  private Job prepareJob(String inputDir, String layoutFile, int totalVtx,
    int iterationCount, int jobCount, int numMapTasks, int partiitonPerWorker, String outputDirPath,
    Configuration configuration) throws IOException, URISyntaxException,
    InterruptedException, ClassNotFoundException {
    Job job = new Job(configuration, "fr-" + jobCount);
    Configuration jobConfig = job.getConfiguration();
    Path outputDir = new Path(outputDirPath);
    FileInputFormat.setInputPaths(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);
    jobConfig.set(FRConstants.LAYOUT_FILE, layoutFile);
    jobConfig.setInt(FRConstants.ITERATION, iterationCount);
    jobConfig.setInt(FRConstants.TOTAL_VTX, totalVtx);
    jobConfig.setInt(FRConstants.NUM_MAPS, numMapTasks);
    jobConfig.setInt(FRConstants.PARTITION_PER_WORKER, partiitonPerWorker);
    jobConfig.setInt("mapreduce.job.max.split.locations", 10000);
    // input class to file-based class
    // job.setInputFormatClass(DataFileInputFormat.class);
    job.setInputFormatClass(MultiFileInputFormat.class);
    // job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setJarByClass(FRJobLauncher.class);
    job.setMapperClass(FRCollectiveMapper.class);
    org.apache.hadoop.mapred.JobConf jobConf = (JobConf) job.getConfiguration();
    jobConf.set("mapreduce.framework.name", "map-collective");
    jobConf.setNumMapTasks(numMapTasks);
    job.setNumReduceTasks(0);
    return job;
  }

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FRJobLauncher(), argv);
    System.exit(res);
  }
}
