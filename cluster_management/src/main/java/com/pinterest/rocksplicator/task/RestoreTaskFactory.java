package com.pinterest.rocksplicator.task;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;

public class RestoreTaskFactory implements TaskFactory {

  @Override
  public Task createNewTask(TaskCallbackContext taskCallbackContext) {
    return new RestoreTask();
  }
}
