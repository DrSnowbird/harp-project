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

package edu.iu.harp.depl;

import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public class Nodes {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(Nodes.class);

  /** Rack ID and its related nodes */
  private Int2ObjectOpenHashMap<ObjectArrayList<String>> nodes;
  /** Maintain the order of racks */
  private IntArrayList nodeRackIDs;
  /** The number of physical nodes */
  private int numPhysicalNodes;

  public Nodes() throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(
      QuickDeployment.nodes_file));
    initializeNodes(reader);
    reader.close();
  }

  public Nodes(BufferedReader reader) throws Exception {
    if (reader == null) {
      LOG.info("Read from default nodes file." + QuickDeployment.nodes_file);
      reader = new BufferedReader(new FileReader(QuickDeployment.nodes_file));
      initializeNodes(reader);
      reader.close();
    } else {
      initializeNodes(reader);
    }
  }

  private void initializeNodes(BufferedReader reader) throws Exception {
    nodes = new Int2ObjectOpenHashMap<ObjectArrayList<String>>();
    nodeRackIDs = new IntArrayList();
    int currentRackID = 0;
    ObjectArrayList<String> nodeSet = new ObjectArrayList<String>();
    try {
      String line = null;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (QuickDeployment.isRack(line)) {
          currentRackID = QuickDeployment.getRackID(line);
          // Check if this rack id exists
          if (!nodes.containsKey(currentRackID)) {
            nodeRackIDs.add(currentRackID);
          }
        } else if (!line.equals("")) {
          addNode(currentRackID, line);
          // Check if this is a new physical node
          if (!nodeSet.contains(line)) {
            nodeSet.add(line);
          }
        }
      }
      numPhysicalNodes = nodeSet.size();
    } catch (Exception e) {
      LOG.error("Errors when reading nodes information.", e);
      throw e;
    }
  }

  private void addNode(int rackID, String line) {
    ObjectArrayList<String> nodeList = nodes.get(rackID);
    // Add list
    if (nodeList == null) {
      nodeList = new ObjectArrayList<String>();
      nodes.put(rackID, nodeList);
    }
    // If the node exists, put them close.
    int pos = nodeList.indexOf(line);
    if (pos > 0) {
      nodeList.add(pos, line);
    } else {
      nodeList.add(line);
    }
  }

  public int getNumPhysicalNodes() {
    return numPhysicalNodes;
  }

  protected Int2ObjectOpenHashMap<ObjectArrayList<String>> getNodes() {
    return this.nodes;
  }

  protected IntArrayList getRackList() {
    return this.nodeRackIDs;
  }

  public List<String> getNodeList() {
    List<String> nodeList = new ArrayList<String>();
    for (Entry<Integer, ObjectArrayList<String>> entry : nodes.entrySet()) {
      nodeList.addAll(entry.getValue());
    }
    return nodeList;
  }

  void sortRacks() {
    nodeRackIDs = new IntArrayList();
    Int2IntLinkedOpenHashMap sortedRacks = new Int2IntLinkedOpenHashMap(
      nodes.size());
    // Sort racks based on sizes in Natural ordering
    for (Entry<Integer, ObjectArrayList<String>> entry : nodes.entrySet()) {
      sortedRacks.put(entry.getValue().size(), entry.getKey().intValue());
    }
    // Put to list
    IntArrayList rackIDs = new IntArrayList(sortedRacks.values());
    for (int i = rackIDs.size() - 1; i >= 0; i--) {
      nodeRackIDs.add(rackIDs.getInt(i));
    }
  }

  List<String> printToNodesFile() {
    List<String> rackNodeList = new ArrayList<String>();
    for (int rackID : nodeRackIDs) {
      rackNodeList.add("#" + rackID);
      rackNodeList.addAll(nodes.get(rackID));
    }
    return rackNodeList;
  }
}
