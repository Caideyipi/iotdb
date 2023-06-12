package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.timecho.iotdb.os.cache.CacheFileManager;
import com.timecho.iotdb.os.cache.OSFileCache;

public class ObjectStorageMetrics implements IMetricSet {
  private final OSFileCache cache;
  private final CacheFileManager cacheFileManager;
  private static final String CACHE_HIT = "cache_hit";
  private static final String CACHE_SIZE = "cache_size";

  public ObjectStorageMetrics() {
    cache = OSFileCache.getInstance();
    cacheFileManager = CacheFileManager.getInstance();
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.OBJECT_STORAGE_CACHE.toString(),
        MetricLevel.IMPORTANT,
        cache,
        OSFileCache::getHitRate,
        Tag.NAME.toString(),
        CACHE_HIT);
    metricService.createAutoGauge(
        Metric.OBJECT_STORAGE_CACHE.toString(),
        MetricLevel.IMPORTANT,
        cacheFileManager,
        CacheFileManager::getTotalCacheFileSize,
        Tag.NAME.toString(),
        CACHE_SIZE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.OBJECT_STORAGE_CACHE.toString(),
        Tag.NAME.toString(),
        CACHE_HIT);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.OBJECT_STORAGE_CACHE.toString(),
        Tag.NAME.toString(),
        CACHE_SIZE);
  }
}
