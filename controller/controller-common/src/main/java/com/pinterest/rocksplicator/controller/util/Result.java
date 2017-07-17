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

package com.pinterest.rocksplicator.controller.util;

/**
 * A data structure for holding the result from TaskQueue so that
 * we can pass the error message to the upstream caller.
 * It contains the actual data returned by the functions
 * and an additional message field.
 *
 * @author Evening Wang (evening@pinterest.com)
 */
public class Result<T> {

  private int statusCode;
  private String message;
  private T data;

  public Result() {
    
  }

  public Result(T data) {
    this.statusCode = -1;
    this.message = "";
    this.data = data;
  }

  public Result(int statusCode, T data) {
    this.statusCode = statusCode;
    this.message = "";
    this.data = data;
  }

  public Result(int statusCode, String message, T data) {
    this.statusCode = statusCode;
    this.message = message;
    this.data = data;
  }

  public int getStatusCode() {
    return this.statusCode;
  }

  public String getMessage() {
    return this.message;
  }

  public T getData() {
    return this.data;
  }

  public Result setStatusCode(int statusCode) {
    this.statusCode = statusCode;
    return this;
  }

  public Result setMessage(String message) {
    this.message = message;
    return this;
  }

  public Result setData(T data) {
    this.data = data;
    return this;
  }
}
