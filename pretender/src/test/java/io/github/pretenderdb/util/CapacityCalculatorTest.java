/*
 * Copyright (c) 2025. Ned Wolpert
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pretenderdb.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pretenderdb.converter.AttributeValueConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;

/**
 * Unit tests for CapacityCalculator.
 */
class CapacityCalculatorTest {

  private CapacityCalculator calculator;

  @BeforeEach
  void setup() {
    final ObjectMapper objectMapper = new ObjectMapper();
    final AttributeValueConverter converter = new AttributeValueConverter(objectMapper);
    calculator = new CapacityCalculator(converter);
  }

  @Test
  void calculateReadCapacity_smallItem_returnsOneRCU() {
    // Small item (well under 4KB)
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "name", AttributeValue.builder().s("Test").build()
    );

    final ConsumedCapacity capacity = calculator.calculateReadCapacity("TestTable", item);

    assertThat(capacity.tableName()).isEqualTo("TestTable");
    assertThat(capacity.capacityUnits()).isEqualTo(1.0);
    assertThat(capacity.readCapacityUnits()).isEqualTo(1.0);
  }

  @Test
  void calculateReadCapacity_largeItem_returnsMultipleRCU() {
    // Create an item larger than 4KB
    final String largeValue = "x".repeat(5000); // 5KB of data
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "data", AttributeValue.builder().s(largeValue).build()
    );

    final ConsumedCapacity capacity = calculator.calculateReadCapacity("TestTable", item);

    assertThat(capacity.tableName()).isEqualTo("TestTable");
    // 5KB should require 2 RCU (rounded up from 1.25)
    assertThat(capacity.capacityUnits()).isGreaterThanOrEqualTo(2.0);
    assertThat(capacity.readCapacityUnits()).isGreaterThanOrEqualTo(2.0);
  }

  @Test
  void calculateReadCapacity_emptyItem_returnsZeroRCU() {
    final Map<String, AttributeValue> item = Map.of();

    final ConsumedCapacity capacity = calculator.calculateReadCapacity("TestTable", item);

    assertThat(capacity.tableName()).isEqualTo("TestTable");
    assertThat(capacity.capacityUnits()).isEqualTo(0.0);
    assertThat(capacity.readCapacityUnits()).isEqualTo(0.0);
  }

  @Test
  void calculateWriteCapacity_smallItem_returnsOneWCU() {
    // Small item (under 1KB)
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "name", AttributeValue.builder().s("Test").build()
    );

    final ConsumedCapacity capacity = calculator.calculateWriteCapacity("TestTable", item);

    assertThat(capacity.tableName()).isEqualTo("TestTable");
    assertThat(capacity.capacityUnits()).isEqualTo(1.0);
    assertThat(capacity.writeCapacityUnits()).isEqualTo(1.0);
  }

  @Test
  void calculateWriteCapacity_largeItem_returnsMultipleWCU() {
    // Create an item larger than 1KB
    final String largeValue = "x".repeat(2000); // 2KB of data
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "data", AttributeValue.builder().s(largeValue).build()
    );

    final ConsumedCapacity capacity = calculator.calculateWriteCapacity("TestTable", item);

    assertThat(capacity.tableName()).isEqualTo("TestTable");
    // 2KB should require at least 2 WCU
    assertThat(capacity.capacityUnits()).isGreaterThanOrEqualTo(2.0);
    assertThat(capacity.writeCapacityUnits()).isGreaterThanOrEqualTo(2.0);
  }

  @Test
  void calculateWriteCapacity_emptyItem_returnsZeroWCU() {
    final Map<String, AttributeValue> item = Map.of();

    final ConsumedCapacity capacity = calculator.calculateWriteCapacity("TestTable", item);

    assertThat(capacity.tableName()).isEqualTo("TestTable");
    assertThat(capacity.capacityUnits()).isEqualTo(0.0);
    assertThat(capacity.writeCapacityUnits()).isEqualTo(0.0);
  }

  @Test
  void calculateReadCapacity_nullItem_returnsZeroRCU() {
    final ConsumedCapacity capacity = calculator.calculateReadCapacity("TestTable", null);

    assertThat(capacity.tableName()).isEqualTo("TestTable");
    assertThat(capacity.capacityUnits()).isEqualTo(0.0);
    assertThat(capacity.readCapacityUnits()).isEqualTo(0.0);
  }

  @Test
  void calculateWriteCapacity_nullItem_returnsZeroWCU() {
    final ConsumedCapacity capacity = calculator.calculateWriteCapacity("TestTable", null);

    assertThat(capacity.tableName()).isEqualTo("TestTable");
    assertThat(capacity.capacityUnits()).isEqualTo(0.0);
    assertThat(capacity.writeCapacityUnits()).isEqualTo(0.0);
  }
}
