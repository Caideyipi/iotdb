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

package org.apache.iotdb.db.pipe.resource.log;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeLogManager {
  private final ConcurrentMap<Class<?>, PipeLogStatus> logClass2LogStatusMap =
      new ConcurrentHashMap<>();

  public void register(Class<?> logClass, int maxAverageScale, int maxLogInterval) {
    logClass2LogStatusMap.put(logClass, new PipeLogStatus(maxAverageScale, maxLogInterval));
  }

  public boolean schedule(Class<?> clazz, int scale) {
    return logClass2LogStatusMap.get(clazz).schedule(scale);
  }
}
