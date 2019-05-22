/// Copyright 2016 Pinterest Inc.
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
///
/// @author anshulp (pundir@pinterest.com)
///

/**
 * Unit tests for status_server
 */

#include "common/stats/status_server.h"

#include <string>
#include <unordered_map>
#include <vector>

#include "curl/curl.h"
#include "gtest/gtest.h"
#include <microhttpd.h>

size_t WriteResponse(void *ptr, size_t size, size_t nmemb, std::string s)
{
  s.append(static_cast<const char* >(ptr));
  return size*nmemb;
}

TEST(StatusServerTest, BasicTest) {
  common::StatusServer::EndPointToOPMap endpoint_to_op = {
      {
          "/success.txt",
          [](const std::vector<std::pair<std::string, std::string>>* v) {
            return std::string("success");
          }
      },
      {
          "/sum",
          [](const std::vector<std::pair<std::string, std::string>>* v) {
            return std::to_string(std::stoi(v->at(0).second) +
                                      std::stoi(v->at(1).second));
          }
      },
      {
          "/divide",
          [](const std::vector<std::pair<std::string, std::string>>* v) {
            std::unordered_map<std::string, int> params;
            for (auto param : *v) {
              params[param.first] = std::stoi(param.second);
            }

            return std::to_string(params["divident"]/params["divisor"]);
          }
      },
  };

  common::StatusServer::StartStatusServer(std::move(endpoint_to_op));

  CURL *c;
  CURLcode errornum;
  long resp;
  std::string s;

  c = curl_easy_init();
  curl_easy_setopt(c, CURLOPT_URL, "http://localhost:9999/success.txt");
  curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, WriteResponse);
  curl_easy_setopt(c, CURLOPT_WRITEDATA, &s);

  errornum = curl_easy_perform(c);
  EXPECT_EQ(errornum, CURLE_OK);

  curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &resp);
  EXPECT_EQ(resp, MHD_HTTP_OK);
  EXPECT_EQ(s, "success");

  curl_easy_cleanup(c);
  s.clear();

  // Sum
  c = curl_easy_init();
  std::string url = "http://localhost:9999/sum?a=10&b=2";
  curl_easy_setopt(c, CURLOPT_URL, url.c_str());
  curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, WriteResponse);
  curl_easy_setopt(c, CURLOPT_WRITEDATA, &s);

  errornum = curl_easy_perform(c);
  EXPECT_EQ(errornum, CURLE_OK);

  curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &resp);
  EXPECT_EQ(resp, MHD_HTTP_OK);
  EXPECT_EQ(s, "12");

  curl_easy_cleanup(c);
  s.clear();

  // Divide
  c = curl_easy_init();
  url = "http://localhost:9999/divide?divident=10&divisor=2";
  curl_easy_setopt(c, CURLOPT_URL, url.c_str());
  curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, WriteResponse);
  curl_easy_setopt(c, CURLOPT_WRITEDATA, &s);

  errornum = curl_easy_perform(c);
  EXPECT_EQ(errornum, CURLE_OK);

  curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &resp);
  EXPECT_EQ(resp, MHD_HTTP_OK);
  EXPECT_EQ(s, "5");

  curl_easy_cleanup(c);
  s.clear();

  // Sum with escaped query params
  c = curl_easy_init();
  std::string params = "?a=10&b=2";
  params = curl_easy_escape(c, params.c_str(), params.size());
  std::string server = "http://localhost:9999/sum";
  curl_easy_setopt(c, CURLOPT_URL, server.append(params).c_str());
  curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, WriteResponse);
  curl_easy_setopt(c, CURLOPT_WRITEDATA, &s);

  errornum = curl_easy_perform(c);
  EXPECT_EQ(errornum, CURLE_OK);

  curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &resp);
  EXPECT_EQ(resp, MHD_HTTP_OK);
  EXPECT_EQ(s, "12");

  curl_easy_cleanup(c);
  s.clear();

  // Divide with escaped query params
  c = curl_easy_init();
  params = "?divident=10&divisor=2";
  params = curl_easy_escape(c, params.c_str(), params.size());
  server = "http://localhost:9999/divide";
  curl_easy_setopt(c, CURLOPT_URL, server.append(params).c_str());
  curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, WriteResponse);
  curl_easy_setopt(c, CURLOPT_WRITEDATA, &s);

  errornum = curl_easy_perform(c);
  EXPECT_EQ(errornum, CURLE_OK);

  curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &resp);
  EXPECT_EQ(resp, MHD_HTTP_OK);
  EXPECT_EQ(s, "5");

  curl_easy_cleanup(c);
  s.clear();

  // Failure test.
  c = curl_easy_init();
  curl_easy_setopt(c, CURLOPT_URL, "http://localhost:9999/failure");
  curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, WriteResponse);
  curl_easy_setopt(c, CURLOPT_WRITEDATA, &s);

  errornum = curl_easy_perform(c);
  EXPECT_EQ(errornum, CURLE_OK);

  curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &resp);
  EXPECT_EQ(resp, MHD_HTTP_OK);
  EXPECT_EQ(s, "Unsupported http path: /failure!\n");

  curl_easy_cleanup(c);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}