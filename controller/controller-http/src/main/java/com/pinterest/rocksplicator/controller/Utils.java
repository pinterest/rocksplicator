/*
 * Copyright 2017 Pinterest, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.rocksplicator.controller;

import javax.ws.rs.core.Response;

/**
 * @author Evening Wang (evening@pinterest.com)
 */
public class Utils {

  public static Response buildResponse(int status, Object body) {
	return Response.status(status).entity(body)
            .header("Access-Control-Allow-Origin", "https://controllerboard.pinadmin.com")
            .header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, OPTIONS")
            .build();
  }
  
}
