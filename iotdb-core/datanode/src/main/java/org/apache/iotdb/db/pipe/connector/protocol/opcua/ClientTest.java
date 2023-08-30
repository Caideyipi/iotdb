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

package org.apache.iotdb.db.pipe.connector.protocol.opcua;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExtensionObject;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.ContentFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.EventFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.SimpleAttributeOperand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

public class ClientTest implements ClientExample {

  public static void main(String[] args) throws Exception {
    ClientTest example = new ClientTest();

    new ClientExampleRunner(example).run();
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final AtomicLong clientHandles = new AtomicLong(1L);

  @Override
  public void run(OpcUaClient client, CompletableFuture<OpcUaClient> future) throws Exception {
    // synchronous connect
    client.connect().get();

    // create a subscription and a monitored item
    UaSubscription subscription = client.getSubscriptionManager().createSubscription(1000.0).get();

    ReadValueId readValueId =
        new ReadValueId(
            Identifiers.Server, AttributeId.EventNotifier.uid(), null, QualifiedName.NULL_VALUE);

    // client handle must be unique per item
    UInteger clientHandle = uint(clientHandles.getAndIncrement());

    EventFilter eventFilter =
        new EventFilter(
            new SimpleAttributeOperand[] {
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "EventId")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "EventType")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "Time")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "Message")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "SourceName")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "SourceNode")},
                  AttributeId.Value.uid(),
                  null)
            },
            new ContentFilter(null));

    MonitoringParameters parameters =
        new MonitoringParameters(
            clientHandle,
            0.0,
            ExtensionObject.encode(client.getStaticSerializationContext(), eventFilter),
            uint(10),
            true);

    MonitoredItemCreateRequest request =
        new MonitoredItemCreateRequest(readValueId, MonitoringMode.Reporting, parameters);

    List<UaMonitoredItem> items =
        subscription
            .createMonitoredItems(TimestampsToReturn.Both, Collections.singletonList(request))
            .get();

    // do something with the value updates
    UaMonitoredItem monitoredItem = items.get(0);

    final AtomicInteger eventCount = new AtomicInteger(0);

    monitoredItem.setEventConsumer(
        (item, vs) -> {
          logger.info("Event Received from {}", item.getReadValueId().getNodeId());

          for (int i = 0; i < vs.length; i++) {
            logger.info("\tvariant[{}]: {}", i, vs[i].getValue());
          }

          if (eventCount.incrementAndGet() == 100) {
            future.complete(client);
          }
        });
  }
}
