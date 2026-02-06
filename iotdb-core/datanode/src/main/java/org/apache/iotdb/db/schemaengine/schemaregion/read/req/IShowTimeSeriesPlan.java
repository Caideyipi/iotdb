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
 *
 */

package org.apache.iotdb.db.schemaengine.schemaregion.read.req;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import java.util.Map;

public interface IShowTimeSeriesPlan extends IShowSchemaPlan {

  boolean needViewDetail();

  SchemaFilter getSchemaFilter();

  Map<Integer, Template> getRelatedTemplate();

  /**
   * Whether to skip invalid series (DISABLED=true) in the result. Default is true.
   *
   * @return true if invalid series should be skipped, false otherwise
   */
  boolean shouldSkipInvalidSchema();

  /**
   * Whether to only return invalid series (DISABLED=true) in the result. Default is false. If true,
   * only invalid series will be returned. This takes precedence over shouldSkipInvalidSchema.
   *
   * @return true if only invalid series should be returned, false otherwise
   */
  boolean shouldOnlyInvalidSchema();

  /** Ordering of timeseries full path in this region, null means no ordering. */
  Ordering getTimeseriesOrdering();
}
