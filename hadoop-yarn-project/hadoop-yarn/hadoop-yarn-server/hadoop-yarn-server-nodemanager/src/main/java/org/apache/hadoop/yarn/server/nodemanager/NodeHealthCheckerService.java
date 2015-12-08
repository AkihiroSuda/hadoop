/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.NodeHealthScriptRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * The class which provides functionality of checking the health of the node and
 * reporting back to the service for which the health checker has been asked to
 * report.
 */
public class NodeHealthCheckerService extends CompositeService {
  private static final Log LOG = LogFactory.getLog(NodeHealthCheckerService.class);

  private NodeHealthScriptRunner nodeHealthScriptRunner;
  private LocalDirsHandlerService dirsHandler;
  private long diskHealthCheckInterval;
  private long diskHealthCheckTimeout;

  static final String SEPARATOR = ";";

  public NodeHealthCheckerService(NodeHealthScriptRunner scriptRunner,
      LocalDirsHandlerService dirHandlerService) {
    super(NodeHealthCheckerService.class.getName());
    nodeHealthScriptRunner = scriptRunner;
    dirsHandler = dirHandlerService;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (nodeHealthScriptRunner != null) {
      addService(nodeHealthScriptRunner);
    }
    addService(dirsHandler);
    this.diskHealthCheckInterval =
        conf.getLong(YarnConfiguration.NM_DISK_HEALTH_CHECK_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_DISK_HEALTH_CHECK_INTERVAL_MS);
    this.diskHealthCheckTimeout =
        conf.getLong(YarnConfiguration.NM_DISK_HEALTH_CHECK_TIMEOUT_MS,
            YarnConfiguration.DEFAULT_NM_DISK_HEALTH_CHECK_TIMEOUT_MS);
    super.serviceInit(conf);
  }

  /**
   * @return the reporting string of health of the node
   */
  String getHealthReport() {
    String scriptReport = (nodeHealthScriptRunner == null) ? ""
        : nodeHealthScriptRunner.getHealthReport();
    if (scriptReport.equals("")) {
      return dirsHandler.getDisksHealthReport(false);
    } else {
      return scriptReport.concat(SEPARATOR + dirsHandler.getDisksHealthReport(false));
    }
  }

  private boolean areDisksCheckedInModerateTime() {
    long diskCheckTime = dirsHandler.getLastDisksCheckTime();
    long now = System.currentTimeMillis();
    long gap = now - diskCheckTime;
    if (gap < 0) {
      throw new AssertionError("implementation error - now=" + now
          + ", diskCheckTime=" + diskCheckTime);
    }
    long allowedGap = this.diskHealthCheckInterval + this.diskHealthCheckTimeout;
    if (allowedGap <= 0) {
      throw new AssertionError("implementation error - interval=" + this.diskHealthCheckInterval
          + ", timeout=" + this.diskHealthCheckTimeout);
    }
    boolean moderate = gap < allowedGap;
    if (!moderate) {
      LOG.warn("the disk seems too slow. gap=" + gap + " (should be less than " + allowedGap + ")");
    }
    return moderate;
  }

  /**
   * @return <em>true</em> if the node is healthy
   */
  boolean isHealthy() {
    boolean scriptHealthStatus = (nodeHealthScriptRunner == null) ? true
        : nodeHealthScriptRunner.isHealthy();
    boolean diskHealthStatus = dirsHandler.areDisksHealthy();
    boolean diskHealthTimeStatus = areDisksCheckedInModerateTime();
    return scriptHealthStatus && diskHealthStatus && diskHealthTimeStatus;
  }

  /**
   * @return when the last time the node health status is reported
   */
  long getLastHealthReportTime() {
    long diskCheckTime = dirsHandler.getLastDisksCheckTime();
    long lastReportTime = (nodeHealthScriptRunner == null)
        ? diskCheckTime
        : Math.max(nodeHealthScriptRunner.getLastReportedTime(), diskCheckTime);
    return lastReportTime;
  }

  /**
   * @return the disk handler
   */
  public LocalDirsHandlerService getDiskHandler() {
    return dirsHandler;
  }

  /**
   * @return the node health script runner
   */
  NodeHealthScriptRunner getNodeHealthScriptRunner() {
    return nodeHealthScriptRunner;
  }
}
