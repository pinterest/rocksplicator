package com.pinterest.rocksplicator.controller.tasks;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This task is used to e2e test controller framework is set up.
 * It does nothing but log the task body.
 *
 */
public class LoggingTask extends AbstractTask<LoggingTask.Param> {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingTask.class);

  public LoggingTask(String messageToLog) {
    this(new Param().setMessageToLog(messageToLog));
  }

  public LoggingTask(Param param) {
    super(param);
  }

  @Override
  public void process(Context ctx) throws Exception {
    LOG.info("Got a message from LoggingTask: " + getParameter().getMessageToLog());
    ctx.getTaskQueue().finishTask(ctx.getId(), "Finsihed logging the message on worker side");
  }

  public static class Param extends Parameter {

    @JsonProperty
    private String messageToLog;

    public String getMessageToLog() {
      return messageToLog;
    }

    public Param setMessageToLog(String messageToLog) {
      this.messageToLog = messageToLog;
      return this;
    }
  }
}
