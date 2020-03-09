package com.pinterest.rocksplicator.task;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestoreTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(RestoreTask.class);

  @Override
  public TaskResult run() {
    LOG.error("RestoreTask run");
    return new TaskResult(TaskResult.Status.COMPLETED, "RestoreTask is completed!");
  }

  @Override
  public void cancel() {
    LOG.error("RestoreTask cancel");
  }
}
