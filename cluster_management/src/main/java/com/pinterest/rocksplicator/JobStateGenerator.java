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

public class JobStateGenerator extends Generator {

  private static final Logger LOG = LoggerFactory.getLogger(JobStateGenerator.class);

  // params to update task related states
  protected final String statePostUrl;

  public JobStateGenerator(String clusterName, HelixManager helixManager,
                           String statePostUrl) {
    super(clusterName, helixManager);

    this.dataParameters.put("author", "WorkflowSateGenerator");
    this.dataParameters.put("comment", "new shard config and workflow states");
    this.statePostUrl = statePostUrl;
    this.driver = new TaskDriver(helixManager);
  }

  @Override
  public void onCallback(NotificationContext notificationContext) {
    LOG.error("Received notification: " + notificationContext.getChangeType());
    if (notificationContext.getChangeType() == HelixConstants.ChangeType.EXTERNAL_VIEW) {
      generateJobStateConfig();
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void onExternalViewChange(List<ExternalView> externalViewList,
                                   NotificationContext changeContext) {
    generateJobStateConfig();
  }


  private void generateJobStateConfig() {
    String newContent = getJobStateConfig();
    postToUrl(newContent, statePostUrl);
  }

}
