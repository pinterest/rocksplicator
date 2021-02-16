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
// @author Gopal Rajpurohit (grajpurohit@pinterest.com)
//

package com.pinterest.rocksplicator.publisher;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a shard_map in json string, post the contents to http service on provided url.
 */
public class HttpPostShardMapPublisher implements ShardMapPublisher<String> {

  private static final Logger LOG = LoggerFactory.getLogger(HttpPostShardMapPublisher.class);

  private final JSONObject dataParameters;
  private final String postUrl;

  public HttpPostShardMapPublisher(final String configPostUrl) {
    this.postUrl = configPostUrl;
    this.dataParameters = new JSONObject();
    this.dataParameters.put("config_version", "v3");
    this.dataParameters.put("author", "ConfigGenerator");
    this.dataParameters.put("comment", "new shard config");
    this.dataParameters.put("content", "{}");
  }

  public void publish(String jsonStringShardMapNewContent) {
    // Write the config to ZK
    LOG.error("Generating a new shard config...");

    this.dataParameters.remove("content");
    this.dataParameters.put("content", jsonStringShardMapNewContent);
    HttpPost httpPost = new HttpPost(this.postUrl);

    try {
      httpPost.setEntity(new StringEntity(this.dataParameters.toString()));
      HttpResponse response = new DefaultHttpClient().execute(httpPost);
      if (response.getStatusLine().getStatusCode() == 200) {
        LOG.error("Succeed to generate a new shard config");
      } else {
        LOG.error(response.getStatusLine().getReasonPhrase());
      }
    } catch (Exception e) {
      LOG.error("Failed to post the new config", e);
    }
  }
}
