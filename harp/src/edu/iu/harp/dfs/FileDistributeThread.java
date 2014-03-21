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

package edu.iu.harp.dfs;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.iu.harp.depl.QuickDeployment;

public class FileDistributeThread implements Runnable {

  private final BlockingQueue<String> hostnameQueue;
  private final Object2ObjectOpenHashMap<String, ObjectArrayList<File>> masterGroups;
  private final Object2ObjectOpenHashMap<String, ObjectArrayList<File>> replicaGroups;
	private String destDirPath;
	private AtomicBoolean error;

	public FileDistributeThread(BlockingQueue<String> hostnameQueue,
    Object2ObjectOpenHashMap<String, ObjectArrayList<File>> masterGroups,
    Object2ObjectOpenHashMap<String, ObjectArrayList<File>> replicaGroups,
    String destDirPath, AtomicBoolean error) {
    this.hostnameQueue = hostnameQueue;
    this.masterGroups = masterGroups;
    this.replicaGroups = replicaGroups; 
    this.destDirPath = destDirPath;
    this.error = error;
  }

	@Override
  public void run() {
    ObjectArrayList<File> files = new ObjectArrayList<File>();
    while (!hostnameQueue.isEmpty()) {
      String hostname = hostnameQueue.poll();
      if (hostname == null) {
        break;
      }
      files.addAll(this.masterGroups.get(hostname));
      files.addAll(this.replicaGroups.get(hostname));
      for (File file : files) {
        if (!QuickDeployment.scpFile(
            file.getAbsolutePath(), hostname + ":" + destDirPath)) {
          this.error.set(true);
          break;
        }
      }
      files.clear();
    }
  }
}
