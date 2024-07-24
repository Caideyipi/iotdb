/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ExtendedPartialPath extends PartialPath {
  final Map<Integer, List<Function<String, Boolean>>> matchFunctions = new HashMap<>();

  public ExtendedPartialPath(final String[] nodes) {
    super(nodes);
  }

  public List<Function<String, Boolean>> getMatchFunctions(final int index) {
    if (!matchFunctions.containsKey(index)) {
      return new ArrayList<>();
    }
    return matchFunctions.get(index);
  }

  public void addMatchFunction(final int index, final Function<String, Boolean> matchFunction) {
    matchFunctions.computeIfAbsent(index, k -> new ArrayList<>()).add(matchFunction);
  }

  public void clearMatchFunction(final int index) {
    matchFunctions.computeIfAbsent(index, k -> new ArrayList<>()).clear();
  }
}
