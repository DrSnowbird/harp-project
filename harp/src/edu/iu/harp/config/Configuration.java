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

package edu.iu.harp.config;

import java.io.FileReader;
import java.util.Properties;

import edu.iu.harp.depl.QuickDeployment;

/**
 * Wrapper class for Project related properties. These properties are read from
 * a file named harp.properties that is available in the classpath.
 * 
 */
public class Configuration {
  // settings
  private final int workerPortBase;
  private final int computeThreadsPerWorker;
  private final String localAppJarDir;
  private final String localDataDir;

  public Configuration() throws Exception {
    Properties properties = new Properties();
    try {
      FileReader reader = new FileReader(QuickDeployment.harp_properties);
      properties.load(reader);
      reader.close();
      this.computeThreadsPerWorker = Integer.parseInt(properties
        .getProperty(QuickDeployment.key_compute_threads_per_worker));
      this.localAppJarDir = properties.getProperty(QuickDeployment.key_app_dir);
      this.localDataDir = properties.getProperty(QuickDeployment.key_data_dir);
      this.workerPortBase = Integer.parseInt(properties
        .getProperty(QuickDeployment.key_worker_port_base));
      // Check for not null
      if (localAppJarDir == null || localDataDir == null || workerPortBase == 0
        || computeThreadsPerWorker == 0) {
        throw new Exception("Invalid properties.");
      }
    } catch (Exception e) {
      throw new Exception("Cannot load the propeties.", e);
    }
  }

  public int getWorkerPortBase() {
    return workerPortBase;
  }

  public String getLocalAppJarDir() {
    return localAppJarDir;
  }

  public String getLocalDataDir() {
    return localDataDir;
  }

  public int getComputeThreadsPerWorker() {
    return computeThreadsPerWorker;
  }
}
