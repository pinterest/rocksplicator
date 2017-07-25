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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.dropwizard.jackson.Jackson;

import java.io.IOException;

/**
 * A marker class which holds all the necessary parameters to run a {@link AbstractTask}.
 *
 * @author Ang Xu (angxu@pinterest.com)
 */
public class Parameter {

  private static final ObjectMapper OBJECT_MAPPER =
      Jackson.newObjectMapper()
             .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

  /**
   * Returns serialized parameters in a json format.
   *
   * @return string of serialized parameter
   * @throws JsonProcessingException
   */
  public String serialize() throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(this);
  }

  /**
   * Converts a json string into a parameter object of type P.
   *
   * @param string serialized json string
   * @param clazz class of Parameter type P
   * @param <P> type of the parameter
   * @return a parameter object of class {@code P}
   * @throws IOException
   */
  public static <P extends Parameter> P deserialize(String string, Class<P> clazz)
      throws IOException {
    return OBJECT_MAPPER.readerFor(clazz).readValue(string);
  }
}
