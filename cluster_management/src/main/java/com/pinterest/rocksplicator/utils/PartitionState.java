/// Copyright 2021 Pinterest Inc.
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
// @author prem (prem@pinterest.com)
//

package com.pinterest.rocksplicator.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class PartitionState {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionState.class);
  public long seqNum = -1;
  public long lastMod = -1;

  public PartitionState() {
  }

  public PartitionState(long seqNum) {
    setSeqNum(seqNum);
  }

  public void clear() {
    seqNum = -1;
    lastMod = -1;
  }

  public void setSeqNum(long seqNum) {
    this.seqNum = seqNum;
    this.lastMod = System.currentTimeMillis();
  }

  public boolean isValid() {
    return seqNum > -1 && lastMod > -1;
  }

  @Override
  public String toString() {
    return String.format("[seq.num=%d lastmod=%d]", seqNum, lastMod);
  }

  public String serialize() {
    JSONObject obj = new JSONObject();
    obj.put("seqnum", seqNum);
    obj.put("lastmod", lastMod);

    return obj.toJSONString();
  }

  public PartitionState deserialize(String serializedData) {
    if (serializedData == null) {
      clear();
    } else {
      JSONParser parser = new JSONParser();
      try {
          JSONObject obj = (JSONObject) parser.parse(serializedData);
          seqNum = ((Long)obj.get("seqnum")).intValue();
          lastMod = ((Long)obj.get("lastmod")).intValue();
      } catch (ParseException e) {
        LOG.error("prem: parse ex" + e );
        e.printStackTrace();
      }
    }
    return this;
  }
} // End of State Class