/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.timecho.iotdb.commons.license.limit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Limit<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(Limit.class);
  private T value;
  private final T valueWithoutLicense;
  CheckedStringParser<T> parser;

  public Limit(T valueWithoutLicense, CheckedStringParser<T> parser) {
    value = valueWithoutLicense;
    this.valueWithoutLicense = valueWithoutLicense;
    this.parser = parser;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public T getValue() {
    return value;
  }
  ;

  /** Use this if license not exist. */
  public void reset() {
    value = valueWithoutLicense;
  }

  public void parse(String valueStr) throws Exception {
    this.value = parser.apply(valueStr);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Limit)) {
      return false;
    }
    return value.equals(((Limit<?>) obj).value);
  }
}
