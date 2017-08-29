/*
 *  Copyright 2017 Pinterest, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.pinterest.rocksplicator.controller.tasks;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class containing states for bad hosts.
 * Sending email rules:
 * For host, it fails the healthcheck for consecutive X times, send email, and mute for Y minutes.
 *
 * @author shu (shu@pinterest.com)
 */

class BadHostState {
  @JsonProperty
  public int consecutiveFailures;
  @JsonProperty
  public Date lastEmailtime;

  public BadHostState() {
    this.consecutiveFailures = 1;
    this.lastEmailtime = null;
  }
}

public class BadHostsStatesKeeper {

  private static final Logger LOG = LoggerFactory.getLogger(BadHostsStatesKeeper.class);

  @NotNull
  private int numFailuresCountAsBad;
  @NotNull
  private int emailMuteIntervalSeconds;

  private long emailMuteIntervalMillis;
  private Map<String, BadHostState> badHostStates = Collections.emptyMap();

  public BadHostsStatesKeeper() {
    this(3, 30 * 60);
  }

  public BadHostsStatesKeeper(int numFailuresCountAsBad, int emailMuteIntervalSeconds) {
    this.numFailuresCountAsBad = numFailuresCountAsBad;
    this.emailMuteIntervalSeconds = emailMuteIntervalSeconds;
    emailMuteIntervalMillis = emailMuteIntervalSeconds * 1000;
  }

  public int getNumFailuresCountAsBad() {
    return numFailuresCountAsBad;
  }

  public BadHostsStatesKeeper setNumFailuresCountAsBad(int numFailuresCountAsBad) {
    this.numFailuresCountAsBad = numFailuresCountAsBad;
    return this;
  }

  public int getEmailMuteIntervalSeconds() {
    return emailMuteIntervalSeconds;
  }

  public BadHostsStatesKeeper setEmailMuteIntervalSeconds(int emailMuteIntervalSeconds) {
    this.emailMuteIntervalSeconds = emailMuteIntervalSeconds;
    return this;
  }

  public Map<String, BadHostState> getBadHostStates() {
    return badHostStates;
  }

  public BadHostsStatesKeeper setBadHostStates(Map<String, BadHostState> badHostStates) {
    this.badHostStates = badHostStates;
    return this;
  }

  /**
   * Swaps and update the current bad host states count, return a list of hosts which should
   * send email this time.
   * @param thisTimeBadHosts
   * @return
   */
  public List<String> updateStatesAndGetHostsToEmail(Set<String> thisTimeBadHosts) {
    Map<String, BadHostState> updatedBadHostStates = new HashMap<>();
    List<String> hostsShouldSendEmail = new ArrayList<>();
    Date currentTime = new Date();
    for (String thisTimeBadHost : thisTimeBadHosts) {
      if (badHostStates.containsKey(thisTimeBadHost)) {
        BadHostState previousState = badHostStates.get(thisTimeBadHost);
        previousState.consecutiveFailures ++;
        if (previousState.consecutiveFailures >= numFailuresCountAsBad &&
            (previousState.lastEmailtime == null ||
                currentTime.getTime() - previousState.lastEmailtime.getTime() > emailMuteIntervalMillis)) {
          previousState.lastEmailtime = currentTime;
          hostsShouldSendEmail.add(thisTimeBadHost);
        }
        updatedBadHostStates.put(thisTimeBadHost, previousState);
      } else {
        updatedBadHostStates.put(thisTimeBadHost, new BadHostState());
      }
    }
    this.badHostStates = updatedBadHostStates;
    return hostsShouldSendEmail;
  }
}
