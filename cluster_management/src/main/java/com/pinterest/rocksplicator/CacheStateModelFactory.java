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
// @author jfang (jfang@pinterest.com)
//

package com.pinterest.rocksplicator;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOG = LoggerFactory.getLogger(CacheStateModelFactory.class);

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    LOG.info("Create a new CACHE state for " + partitionName);
    return new CacheStateModel(resourceName, partitionName);
  }

  public static class CacheStateModel extends StateModel {
    private static final Logger LOG = LoggerFactory.getLogger(CacheStateModel.class);
    private final String resourceName;
    private final String partitionName;

    public CacheStateModel(String resourceName, String partitionName) {
      this.resourceName = resourceName;
      this.partitionName = partitionName;
    }

    /**
     * Callback for OFFLINE to ONLINE transition.
     * This callback does nothing.
     */
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      checkSanity("OFFLINE", "ONLINE", message);
      Utils.logTransitionMessage(message);
    }

    /**
     * Callback for ONLINE to OFFLINE transition.
     * This callback does nothing.
     */
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      checkSanity("ONLINE", "OFFLINE", message);
      Utils.logTransitionMessage(message);
    }

    /**
     * Callback for OFFLINE to DROPPED transition.
     * This callback does nothing
     */
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      checkSanity("OFFLINE", "DROPPED", message);
      Utils.logTransitionMessage(message);
    }

    /**
     * Callback for ERROR to DROPPED transition.
     * This callback does nothing
     */
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      checkSanity("ERROR", "DROPPED", message);
      Utils.logTransitionMessage(message);
    }

    /**
     * Callback for ERROR to OFFLINE transition.
     * This callback does nothing
     */
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      checkSanity("ERROR", "OFFLINE", message);
      Utils.logTransitionMessage(message);
    }

    private void checkSanity(String fromState, String toState, Message message) {
      if (fromState.equalsIgnoreCase(message.getFromState())
          && toState.equalsIgnoreCase(message.getToState())
          && resourceName.equalsIgnoreCase(message.getResourceName())
          && partitionName.equalsIgnoreCase(message.getPartitionName())) {
        return;
      }

      LOG.error("Invalid meesage: " + message.toString());
      LOG.error("From " + fromState + " to " + toState + " for " + partitionName);
    }
  }
}
