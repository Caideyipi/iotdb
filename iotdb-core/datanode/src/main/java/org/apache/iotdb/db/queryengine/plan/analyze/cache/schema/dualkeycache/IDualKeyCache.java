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

package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * This interfaces defines the behaviour of a dual key cache. A dual key cache supports manage cache
 * values via two keys, first key and second key. Simply, the structure is like fk -> sk-> value.
 *
 * @param <FK> The first key of cache value
 * @param <SK> The second key of cache value
 * @param <V> The cache value
 */
public interface IDualKeyCache<FK, SK, V> {

  /** Get the cache value with given first key and second key. */
  V get(FK firstKey, SK secondKey);

  /**
   * Traverse target cache values via given first key and second keys provided in computation and
   * execute the defined computation logic. The computation is read only.
   */
  void compute(IDualKeyCacheComputation<FK, SK, V> computation);

  /**
   * Traverse target cache values via given first key and second keys provided in computation and
   * execute the defined computation logic. Value can be updated in this computation.
   */
  void update(IDualKeyCacheUpdating<FK, SK, V> updating);

  /** put the cache value into cache */
  void put(final FK firstKey, final SK secondKey, final V value);

  /**
   * Update the existing value. The updater shall return the difference caused by the update,
   * because we do not want to call "valueSizeComputer" twice, which may include abundant useless
   * calculations.
   *
   * <p>Warning: This method is without any locks for performance concerns. The caller shall ensure
   * the concurrency safety for the value update.
   *
   * @param createIfNotExists put the value to cache iff it does not exist,
   */
  void update(
      final FK firstKey,
      final SK secondKey,
      final V value,
      final ToIntFunction<V> updater,
      final boolean createIfNotExists);

  /**
   * Update all the existing value with {@link SK} and a the {@link SK}s matching the given
   * predicate. The updater shall return the difference caused by the update, because we do not want
   * to call "valueSizeComputer" twice, which may include abundant useless calculations.
   *
   * <p>Warning: This method is without any locks for performance concerns. The caller shall ensure
   * the concurrency safety for the value update.
   */
  void update(
      final FK firstKey, final Predicate<SK> secondKeyChecker, final ToIntFunction<V> updater);

  /**
   * Invalidate last cache in datanode schema cache. Do not invalidate time series cache.
   *
   * @param partialPathList
   */
  void invalidateLastCache(PartialPath partialPath);

  void invalidateDataRegionLastCache(String database);

  /**
   * Invalidate all cache values in the cache and clear related cache keys. The cache status and
   * statistics won't be clear and they can still be accessed via cache.stats().
   */
  @GuardedBy("DataNodeSchemaCache#writeLock")
  void invalidateAll();

  /**
   * Invalidate cache values in the cache and clear related cache keys. The cache status and
   * statistics won't be clear and they can still be accessed via cache.stats().
   */
  @GuardedBy("DataNodeSchemaCache#writeLock")
  void invalidate(String database);

  /**
   * Invalidate cache values in the cache and clear related cache keys. The cache status and
   * statistics won't be clear and they can still be accessed via cache.stats().
   */
  @GuardedBy("DataNodeSchemaCache#writeLock")
  void invalidate(List<? extends PartialPath> partialPathList);

  /**
   * Clean up all data and info of this cache, including cache keys, cache values and cache stats.
   */
  @GuardedBy("DataNodeSchemaCache#writeLock")
  void cleanUp();

  /** Return all the current cache status and statistics. */
  IDualKeyCacheStats stats();

  @TestOnly
  void evictOneEntry();

  /** remove all entries for firstKey */
  @GuardedBy("DataNodeSchemaCache#writeLock")
  void invalidate(FK firstKey);

  /** remove all entries matching the firstKey and the secondKey */
  @GuardedBy("DataNodeSchemaCache#writeLock")
  void invalidate(final Predicate<FK> firstKeyChecker, final Predicate<SK> secondKeyChecker);
}
