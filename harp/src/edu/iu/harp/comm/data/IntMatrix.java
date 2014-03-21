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

package edu.iu.harp.comm.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.log4j.Logger;

public class IntMatrix extends StructObject {
  /** Class logger */
  private static final Logger LOG =
    Logger.getLogger(IntMatrix.class);
  
  private int[][] matrix;
  private int row;
  private int col;

  public IntMatrix() {
    matrix = null;
    row = 0;
    col = 0;
  }

  public void updateMatrix(int[][] matrixBody, int row, int col) {
    this.matrix = matrixBody;
    this.row = row;
    this.col = col;
  }

  public int[][] getMatrixBody() {
    return this.matrix;
  }
  
  public int getRow() {
    return this.row;
  }

  public int getCol() {
    return this.col;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(row);
    out.writeInt(col);
    for (int i = 0; i < row; i++) {
      for (int j = 0; j < col; j++) {
        out.writeInt(matrix[i][j]);
      }
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    row = in.readInt();
    col = in.readInt();
    if (matrix == null || row > matrix.length || col > matrix[0].length) {
      LOG.info("Create a new matrix.");
      matrix = new int[row][col];
    } else {
      LOG.info("Get an existing matrix.");
    }
    for (int i = 0; i < row; i++) {
      for (int j = 0; j < col; j++) {
        matrix[i][j] = in.readInt();
      }
    }
  }

  @Override
  public int getSizeInBytes() {
    return row * col * 4 + 2 * 4;
  }
}
