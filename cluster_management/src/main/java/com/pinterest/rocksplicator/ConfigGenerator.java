/// Copyright 2017 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author bol (bol@pinterest.com)
//

package com.pinterest.rocksplicator;

import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ConfigGenerator extends Generator {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigGenerator.class);
  
  private final String configPostUrl;

  public ConfigGenerator(String clusterName, HelixManager helixManager, String configPostUrl) {
    super(clusterName, helixManager);
    this.configPostUrl = configPostUrl;
    this.dataParameters.put("author", "ConfigGenerator");
    this.dataParameters.put("comment", "new shard config");
  }

  @Override
  public void onCallback(NotificationContext notificationContext) {
    LOG.error("Received notification: " + notificationContext.getChangeType());

    if (notificationContext.getChangeType() == HelixConstants.ChangeType.EXTERNAL_VIEW) {
      generateShardConfig();
    } else if (notificationContext.getChangeType() == HelixConstants.ChangeType.INSTANCE_CONFIG) {
      if (updateDisabledHosts()) {
        generateShardConfig();
      }
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void onConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
    if (updateDisabledHosts()) {
      generateShardConfig();
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    generateShardConfig();
  }

  private void generateShardConfig() {
    String newContent = getShardConfig();
    postToUrl(newContent, configPostUrl);
  }

}
