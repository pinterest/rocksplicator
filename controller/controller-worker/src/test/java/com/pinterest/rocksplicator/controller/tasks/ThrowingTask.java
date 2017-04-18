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

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class ThrowingTask extends AbstractTask<ThrowingTask.Param> {

  public ThrowingTask(String errorMsg) {
    this(new Param().setErrorMsg(errorMsg));
  }

  public ThrowingTask(Param param) {
    super(param);
  }

  @Override
  public void process(Context ctx) throws Exception {
    throw new Exception(getParameter().getErrorMsg());
  }

  public static class Param extends Parameter {
    @JsonProperty
    private String errorMsg;

    public String getErrorMsg() {
      return errorMsg;
    }

    public Param setErrorMsg(String errorMsg) {
      this.errorMsg = errorMsg;
      return this;
    }
  }
}
