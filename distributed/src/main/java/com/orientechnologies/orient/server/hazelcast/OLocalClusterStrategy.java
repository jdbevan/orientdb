/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *  
 */
package com.orientechnologies.orient.server.hazelcast;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * Distributed cluster selection strategy that always prefers local cluster if any reducing network latency of remote calls. It
 * computes the best cluster the first time and every-time the configuration changes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OLocalClusterStrategy implements OClusterSelectionStrategy {
  public final static String                NAME          = "local";
  protected final String                    localNodeName;
  protected final ODistributedConfiguration cfg;
  protected final OClass                    cls;
  protected int                             bestClusterId = -1;

  public OLocalClusterStrategy(final String localNodeName, final ODistributedConfiguration cfg, final OClass iClass) {
    this.localNodeName = localNodeName;
    this.cfg = cfg;
    this.cls = iClass;
    readConfiguration();
  }

  @Override
  public int getCluster(final OClass iClass) {
    return bestClusterId;
  }

  public void readConfiguration() {
    if (cls.isAbstract())
      return;

    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();

    final int[] clusterIds = cls.getClusterIds();
    final List<String> clusterNames = new ArrayList<String>(clusterIds.length);
    for (int c : clusterIds)
      clusterNames.add(db.getClusterNameById(c));

    final String bestCluster = cfg.getClosestClusterWithLocalMasterServer(clusterNames, localNodeName);

    bestClusterId = db.getClusterIdByName(bestCluster);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
