package com.pinterest.rocksplicator;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.ExternalView;
import org.apache.helix.task.TaskDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ConfigAndJobStateGenerator extends Generator {

  private static final Logger LOG = LoggerFactory.getLogger(JobStateGenerator.class);

  // params to update task related states
  private final String configPostUrl;
  private final String statePostUrl;

  public ConfigAndJobStateGenerator(String clusterName, HelixManager helixManager,
                                    String configPostUrl, String statePostUrl) {
    super(clusterName, helixManager);

    this.dataParameters.put("author", "WorkflowSateGenerator");
    this.dataParameters.put("comment", "new shard config and job states");
    // FIXME: add as general provied url
    this.configPostUrl = configPostUrl;
    this.statePostUrl = statePostUrl;
    this.driver = new TaskDriver(helixManager);
  }

  @Override
  public void onCallback(NotificationContext notificationContext) {
    LOG.error("Received notification: " + notificationContext.getChangeType());
    if (notificationContext.getChangeType() == HelixConstants.ChangeType.EXTERNAL_VIEW) {
      generateConfigAndJobState();
    } else if (notificationContext.getChangeType() == HelixConstants.ChangeType.INSTANCE_CONFIG) {
      if (updateDisabledHosts()) {
        generateShardConfig();
      }
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void onExternalViewChange(List<ExternalView> externalViewList,
                                   NotificationContext changeContext) {
    generateConfigAndJobState();
  }

  private void generateConfigAndJobState() {
    String newConfigContent = getShardConfig();
    postToUrl(newConfigContent, configPostUrl);
    String newJobStateContent = getJobStateConfig();
    postToUrl(newJobStateContent, statePostUrl);
  }
  
  private void generateShardConfig() {
    String newContent = getShardConfig();
    postToUrl(newContent, configPostUrl);
  }
}
