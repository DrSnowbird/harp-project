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

package edu.iu.harp.collective;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

public class TaskCallable<I, O, T extends Task<I, O>> implements
  Callable<Result<O>> {

  /** Class logger */
  // protected static final Logger LOG = Logger.getLogger(TaskCallable.class);

  private final BlockingQueue<I> queue;
  private final T task;
  private final int taskID;

  public TaskCallable(BlockingQueue<I> q, T t, int i) {
    queue = q;
    task = t;
    taskID = i;
  }

  @Override
  public Result<O> call() throws Exception {
    Result<O> result = new Result<O>();
    while (!queue.isEmpty()) {
      I input = queue.poll();
      if (input == null) {
        break;
      }
      O output = task.run(input);
      if (output != null) {
        result.addData(output);
      }
    }
    return result;
  }
}
