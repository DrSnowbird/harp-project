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

package edu.iu.benchmark.bcast;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.iu.common.MultiFileInputFormat;

public class JobLauncher extends Configured implements Tool {

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new Configuration(), new JobLauncher(), argv);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 4) {
      System.err
        .println("Usage: edu.iu.benchmark.bcast.JobLauncher <number of partitions> "
          + "<total number of bytes> <number of mappers> <regenerate data>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    int numPartitions = Integer.parseInt(args[0]);
    long totalBytes = Long.parseLong(args[1]);
    int numMappers = Integer.parseInt(args[2]);
    int numIterations = Integer.parseInt(args[3]);
    boolean regenerateData = true;
    if (args.length == 5) {
      regenerateData = Boolean.parseBoolean(args[4]);
    }
    String workDirName = "bcast";
    launch(numPartitions, totalBytes, numMappers, numIterations,
      regenerateData, workDirName);
    return 0;
  }

  private void launch(int numPartitions, long totalBytes, int numMappers,
    int numIterations, boolean generateData, String workDirName)
    throws IOException, URISyntaxException, InterruptedException,
    ExecutionException, ClassNotFoundException {
    Configuration configuration = getConf();
    FileSystem fs = FileSystem.get(configuration);
    Path workDirPath = new Path(workDirName);
    Path bcastDataDirPath = new Path(workDirPath, "bcast");
    Path dataDirPath = new Path(workDirPath, "data");
    Path outDirPath = new Path(workDirPath, "out");
    if (fs.exists(outDirPath)) {
      fs.delete(outDirPath, true);
    }
    if (generateData) {
      System.out.println("Generate data.");
      DataGen.generateData(totalBytes, numMappers, dataDirPath,
        bcastDataDirPath, "/tmp/bcast/", fs);
    }
    bcastBenchmark(numPartitions, totalBytes, numMappers, numIterations,
      dataDirPath, bcastDataDirPath, outDirPath);
  }

  private void bcastBenchmark(int numPartitions, long totalBytes,
    int numMappers, int numIterations, Path dataDirPath, Path bcastDataDirPath,
    Path outDirPath) {
    try {
      Job bcastJob = configureBcastJob(numPartitions, totalBytes, numMappers,
        numIterations, dataDirPath, bcastDataDirPath, outDirPath);
      bcastJob.waitForCompletion(true);
    } catch (IOException | URISyntaxException | ClassNotFoundException
      | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private Job configureBcastJob(int numPartitions, long totalBytes,
    int numMappers, int numIterations, Path dataDirPath, Path bcastDataDirPath,
    Path outDirPath) throws IOException, URISyntaxException {
    Job job = new Job(getConf(), "bcast_job");
    FileInputFormat.setInputPaths(job, dataDirPath);
    FileOutputFormat.setOutputPath(job, outDirPath);
    job.setInputFormatClass(MultiFileInputFormat.class);
    job.setJarByClass(JobLauncher.class);
    job.setMapperClass(BcastMapper.class);
    org.apache.hadoop.mapred.JobConf jobConf = (JobConf) job.getConfiguration();
    jobConf.set("mapreduce.framework.name", "map-collective");
    jobConf.setNumMapTasks(numMappers);
    job.setNumReduceTasks(0);
    jobConf.setInt(BcastConstants.NUM_PARTITIONS, numPartitions);
    jobConf.setInt(BcastConstants.NUM_MAPPERS, numMappers);
    jobConf.setLong(BcastConstants.TOTAL_BYTES, totalBytes);
    jobConf.setInt(BcastConstants.NUM_ITERATIONS, numIterations);
    return job;
  }
}
