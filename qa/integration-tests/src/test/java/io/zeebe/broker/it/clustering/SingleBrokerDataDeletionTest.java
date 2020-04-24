/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.clustering;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.Broker;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.DataCfg;
import io.zeebe.broker.system.configuration.ExporterCfg;
import io.zeebe.exporter.api.Exporter;
import io.zeebe.exporter.api.context.Controller;
import io.zeebe.protocol.record.Record;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.util.unit.DataSize;

public class SingleBrokerDataDeletionTest {

  private static final Duration SNAPSHOT_PERIOD = Duration.ofMinutes(1);

  @Rule
  public final ClusteringRule clusteringRule =
      new ClusteringRule(1, 1, 1, this::configureCustomExporter);

  private void configureCustomExporter(final BrokerCfg brokerCfg) {
    final DataCfg data = brokerCfg.getData();
    data.setSnapshotPeriod(SNAPSHOT_PERIOD);
    data.setLogSegmentSize(DataSize.ofKilobytes(8));
    data.setLogIndexDensity(5);
    brokerCfg.getNetwork().setMaxMessageSize(DataSize.ofKilobytes(8));

    final ExporterCfg exporterCfg = new ExporterCfg();
    exporterCfg.setClassName(ControllableExporter.class.getName());
    brokerCfg.setExporters(Collections.singletonMap("snapshot-test-exporter", exporterCfg));
  }

  @Test
  public void shouldNotCompactNotExportedEvents() {
    // given
    final Broker broker = clusteringRule.getBroker(0);

    int i = 0;
    while (getSegmentsCount(broker) <= 2) {
      clusteringRule
          .getClient()
          .newPublishMessageCommand()
          .messageName("msg")
          .correlationKey("key")
          .messageId("msg" + (++i))
          .send()
          .join();
    }

    // when

    // last exported position is at-most in segment 2
    ControllableExporter.updatePosition(false);

    // write more events
    while (getSegmentsCount(broker) <= 4) {
      clusteringRule
          .getClient()
          .newPublishMessageCommand()
          .messageName("msg")
          .correlationKey("key")
          .messageId("msg" + (++i))
          .send()
          .join();
    }

    // increase snapshot interval and wait
    clusteringRule.getClock().addTime(SNAPSHOT_PERIOD);
    clusteringRule.waitForSnapshotAtBroker(broker);

    // then
    assertThat(getSegmentsCount(broker)).isGreaterThanOrEqualTo(3);
  }

  private int getSegmentsCount(final Broker broker) {
    return getSegments(broker).size();
  }

  private Collection<Path> getSegments(final Broker broker) {
    try {
      return Files.list(clusteringRule.getSegmentsDirectory(broker))
          .filter(path -> path.toString().endsWith(".log"))
          .collect(Collectors.toList());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static class ControllableExporter implements Exporter {
    static final List<Record> RECORDS = new CopyOnWriteArrayList<>();
    static volatile boolean shouldUpdatePosition = true;
    private Controller controller;

    static void updatePosition(final boolean flag) {
      shouldUpdatePosition = flag;
    }

    @Override
    public void open(final Controller controller) {
      this.controller = controller;
    }

    @Override
    public void export(final Record record) {
      RECORDS.add(record);
      if (shouldUpdatePosition) {
        controller.updateLastExportedRecordPosition(record.getPosition());
      }
    }
  }
}
