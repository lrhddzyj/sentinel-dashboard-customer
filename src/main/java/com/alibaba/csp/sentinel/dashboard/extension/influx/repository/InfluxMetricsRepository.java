package com.alibaba.csp.sentinel.dashboard.extension.influx.repository;

import static com.alibaba.csp.sentinel.dashboard.extension.influx.DatabaseConstant.DATABASE_NAME;
import static com.alibaba.csp.sentinel.dashboard.extension.influx.DatabaseConstant.METRIC_DATA_TABLE_NAME;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.eq;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.gte;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.lte;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.select;

import com.alibaba.csp.sentinel.dashboard.datasource.entity.MetricEntity;
import com.alibaba.csp.sentinel.dashboard.extension.influx.entity.InfluxMetrics;
import com.alibaba.csp.sentinel.dashboard.repository.metric.MetricsRepository;
import com.alibaba.csp.sentinel.util.StringUtil;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBResultMapper;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

/**
 * @description:
 * @author: lrh
 * @date: 2020/9/5 11:34
 */
@Primary
@Repository("influxDBMetricsRepository")
public class InfluxMetricsRepository implements MetricsRepository<MetricEntity> {

  private InfluxDB influxDB;

  /**
   * 北京时间领先UTC时间8小时 UTC: Universal Time Coordinated,世界统一时间
   */
  private static final Integer UTC_8 = 8;

  /**
   * 时间格式
   */
  private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

  public InfluxMetricsRepository(InfluxDB influxDB) {
    this.influxDB = influxDB;
  }

  @Override
  public void save(MetricEntity metric) {
    if (metric == null || StringUtil.isBlank(metric.getApp())) {
      return;
    }
    InfluxMetrics influxMetrics = conversion(metric);
    Point point = Point.measurementByPOJO(InfluxMetrics.class)
        .addFieldsFromPOJO(influxMetrics)
        .time(DateUtils.addHours(metric.getTimestamp(), UTC_8).getTime(),
            TimeUnit.MILLISECONDS)
        .build();

    influxDB.write(point);
  }

  @Override
  public void saveAll(Iterable<MetricEntity> metrics) {

    List<Point> inPoints = StreamSupport.stream(metrics.spliterator(), false)
        .filter(m -> (m != null && StringUtil.isNotBlank(m.getApp())))
        .map(m -> {
              InfluxMetrics influxMetrics = conversion(m);
              Point point = Point.measurementByPOJO(InfluxMetrics.class)
                  .addFieldsFromPOJO(influxMetrics)
                  .time(DateUtils.addHours(m.getTimestamp(), UTC_8).getTime(),
                      TimeUnit.MILLISECONDS)
                  .build();
              return point;
            }
        )
        .collect(Collectors.toList());
    if (CollectionUtils.isEmpty(inPoints)) {
      return;
    }

    BatchPoints batchPoints = BatchPoints.database(DATABASE_NAME).points(inPoints).build();
    influxDB.write(batchPoints);
  }

  private InfluxMetrics conversion(MetricEntity metric) {
    InfluxMetrics influxMetrics = new InfluxMetrics();
    influxMetrics.setId(metric.getId());
    influxMetrics.setGmtCreate(metric.getGmtCreate().getTime());
    influxMetrics.setGmtModified(metric.getGmtModified().getTime());
    influxMetrics.setApp(metric.getApp());
    influxMetrics.setResource(metric.getResource());
    influxMetrics.setPassQps(metric.getPassQps());
    influxMetrics.setSuccessQps(metric.getSuccessQps());
    influxMetrics.setBlockQps(metric.getBlockQps());
    influxMetrics.setExceptionQps(metric.getExceptionQps());
    influxMetrics.setRt(metric.getRt());
    influxMetrics.setCount(metric.getCount());
    influxMetrics.setResourceCode(metric.getResourceCode());
    return influxMetrics;
  }

  private static InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();

  @Override
  public List<MetricEntity> queryByAppAndResourceBetween(String app, String resource, long startTime,
      long endTime) {

    Query query = select()
        .from(DATABASE_NAME,METRIC_DATA_TABLE_NAME)
        .where(eq("app", app))
        .and(eq("resource", resource))
        .and(gte("time", DateFormatUtils.format(new Date(startTime), DATE_FORMAT_PATTERN)))
        .and(lte("time", DateFormatUtils.format(new Date(endTime), DATE_FORMAT_PATTERN)));

    QueryResult queryResult = influxDB.query(query);
    List<InfluxMetrics> influxMetrics = resultMapper.toPOJO(queryResult, InfluxMetrics.class);

    if (CollectionUtils.isEmpty(influxMetrics)) {
      return Collections.EMPTY_LIST;
    }

    return influxMetrics.stream().map(m -> convertToMetricEntity(m)).collect(Collectors.toList());
  }

  @Override
  public List<String> listResourcesOfApp(String app) {
    if (StringUtils.isBlank(app)) {
      return Collections.emptyList();
    }

    //这个时间可以修改
    final long startTime = System.currentTimeMillis() - 1000 * 60 * 5;

    Query query = select().from(DATABASE_NAME,METRIC_DATA_TABLE_NAME)
        .where(eq("app", app))
        .and(gte("time", DateFormatUtils.format(new Date(startTime), DATE_FORMAT_PATTERN)));

    QueryResult queryResult = influxDB.query(query);
    List<InfluxMetrics> influxMetrics = resultMapper.toPOJO(queryResult, InfluxMetrics.class);

    if (CollectionUtils.isEmpty(influxMetrics)) {
      return Collections.emptyList();
    }
    final Map<String, MetricEntity> resourceCount = new HashMap<>(32);
    influxMetrics.stream()
        .map(m -> convertToMetricEntity(m))
        .forEach(e -> {
          String resource = e.getResource();
          if (resourceCount.containsKey(resource)) {
            MetricEntity inMetricEntity = resourceCount.get(resource);
            inMetricEntity.addPassQps(e.getPassQps());
            inMetricEntity.addRtAndSuccessQps(e.getRt(), e.getSuccessQps());
            inMetricEntity.addBlockQps(e.getBlockQps());
            inMetricEntity.addExceptionQps(e.getExceptionQps());
            inMetricEntity.addCount(1);
          } else {
            resourceCount.put(resource, MetricEntity.copyOf(e));
          }

        });

    List<String> resources = resourceCount.entrySet()
        .stream()
        .sorted((o1, o2) -> {
          MetricEntity e1 = o1.getValue();
          MetricEntity e2 = o2.getValue();
          int t = e2.getBlockQps().compareTo(e1.getBlockQps());
          if (t != 0) {
            return t;
          }
          return e2.getPassQps().compareTo(e1.getPassQps());
        })
        .map(Entry::getKey)
        .collect(Collectors.toList());

    return resources;
  }


  private MetricEntity convertToMetricEntity(InfluxMetrics influxMetrics) {
    MetricEntity metricEntity = new MetricEntity();

    metricEntity.setId(influxMetrics.getId());
    metricEntity.setGmtCreate(new Date(influxMetrics.getGmtCreate()));
    metricEntity.setGmtModified(new Date(influxMetrics.getGmtModified()));
    metricEntity.setApp(influxMetrics.getApp());
    metricEntity
        .setTimestamp(Date.from(influxMetrics.getTime().minusMillis(TimeUnit.HOURS.toMillis(UTC_8))));
    metricEntity.setResource(influxMetrics.getResource());
    metricEntity.setPassQps(influxMetrics.getPassQps());
    metricEntity.setSuccessQps(influxMetrics.getSuccessQps());
    metricEntity.setBlockQps(influxMetrics.getBlockQps());
    metricEntity.setExceptionQps(influxMetrics.getExceptionQps());
    metricEntity.setRt(influxMetrics.getRt());
    metricEntity.setCount(influxMetrics.getCount());

    return metricEntity;
  }


}
