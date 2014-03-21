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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class DataGenRunnable implements Runnable {

  private double[] distMat;
  private int width;
  private int height;
  private int row;
  private int rowOffset;
  private String localDir;

  public DataGenRunnable(double[] distMat, String localDir, int width,
    int height, int row, int rowOffset) {
    this.distMat = distMat;
    this.localDir = localDir;
    this.width = width;
    this.height = height;
    this.row = row;
    this.rowOffset = rowOffset;
  }

  @Override
  public void run() {
    String filePath = this.localDir + File.separator + "data_" + this.row;
    try {
      DataOutputStream out = new DataOutputStream(
        new FileOutputStream(filePath));
      // 4 properties
      out.writeInt(height);
      out.writeInt(width);
      out.writeInt(row);
      out.writeInt(rowOffset);
      int start = rowOffset * width;
      int end = (rowOffset + height) * width;
      for (int i = start; i < end; i++) {
        out.writeDouble(distMat[i]);
      }
      out.close();
      System.out.println("Done written file " + filePath);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
