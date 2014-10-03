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

package edu.iu.harp.comm;

public class Constants {
	// Code
	public static final byte RECEIVER_QUIT_REQUEST = 0;
	public static final byte WRITABLE_OBJ_REQUEST_ALT = 1;
	public static final byte WRITABLE_OBJ_CHAIN_BCAST = 2;
	public static final byte BYTE_ARRAY_REQUEST = 3;
	public static final byte BYTE_ARRAY_CHAIN_BCAST = 4;
  public static final byte INT_ARRAY_CHAIN_BCAST_ALT = 5;
  public static final byte INT_ARRAY_CHAIN_BCAST = 6;
  public static final byte WRITABLE_OBJ_REQUEST = 7;
  public static final byte STRUCT_OBJ_REQUEST = 8;
  public static final byte INT_ARRAY_REQUEST = 9;
  public static final byte STRUCT_OBJ_CHAIN_BCAST = 10;
  public static final byte CHAIN_BCAST_ACK = 11;
  public static final byte DOUBLE_ARRAY_REQUEST = 12;
  public static final byte DOUBLE_ARRAY_CHAIN_BCAST = 13;
  
	public static final int DATA_MAX_WAIT_TIME = 1800; // seconds
	
	public static final int CONNECT_MAX_WAIT_TIME = 60000;

	public static final long TERMINATION_TIMEOUT_1 = 60;
	public static final long TERMINATION_TIMEOUT_2 = 60;
  public static final int SENDRECV_BYTE_UNIT = 262144;
  // public static final int SENDRECV_BYTE_UNIT = 11860;
	public static final int SENDRECV_INT_UNIT = 2048;
	public static final int SLEEP_COUNT = 100;
	public static final int RETRY_COUNT = 100;
	
	public static final int NUM_HANDLER_THREADS = 32;
	public static final int NUM_DESERIAL_THREADS = 32;
	public static final int NUM_SENDER_THREADS = 32;
	public static final int DEFAULT_WORKER_POART_BASE = 12800;





  
}
