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

import io.github.pretenderdb.converter.AttributeValueConverter;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Capacity;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;

/**
 * Calculates consumed capacity units for DynamoDB operations.
 *
 * <p>Capacity is calculated based on DynamoDB pricing model:
 * <ul>
 *   <li><strong>Read operations:</strong> 1 RCU per 4 KB (strongly consistent reads)</li>
 *   <li><strong>Write operations:</strong> 1 WCU per 1 KB</li>
 * </ul>
 *
 * <p>Note: Pretender always performs strongly consistent reads, so there is no
 * 0.5 RCU for eventually consistent reads.
 */
@Singleton
public class CapacityCalculator {

  private static final int READ_CAPACITY_UNIT_SIZE = 4096;  // 4 KB
  private static final int WRITE_CAPACITY_UNIT_SIZE = 1024; // 1 KB

  private final AttributeValueConverter converter;

  /**
   * Instantiates a new Capacity calculator.
   *
   * @param converter the converter
   */
  @Inject
  public CapacityCalculator(final AttributeValueConverter converter) {
    this.converter = converter;
  }

  /**
   * Calculate consumed capacity for a read operation.
   *
   * @param tableName the table name
   * @param item      the item that was read
   * @return the consumed capacity
   */
  public ConsumedCapacity calculateReadCapacity(final String tableName,
                                                final Map<String, AttributeValue> item) {
    if (item == null || item.isEmpty()) {
      return ConsumedCapacity.builder()
          .tableName(tableName)
          .capacityUnits(0.0)
          .readCapacityUnits(0.0)
          .build();
    }

    final double capacityUnits = calculateReadCapacityUnits(item);

    return ConsumedCapacity.builder()
        .tableName(tableName)
        .capacityUnits(capacityUnits)
        .readCapacityUnits(capacityUnits)
        .table(Capacity.builder().capacityUnits(capacityUnits).readCapacityUnits(capacityUnits).build())
        .build();
  }

  /**
   * Calculate consumed capacity for a write operation.
   *
   * @param tableName the table name
   * @param item      the item that was written
   * @return the consumed capacity
   */
  public ConsumedCapacity calculateWriteCapacity(final String tableName,
                                                 final Map<String, AttributeValue> item) {
    if (item == null || item.isEmpty()) {
      return ConsumedCapacity.builder()
          .tableName(tableName)
          .capacityUnits(0.0)
          .writeCapacityUnits(0.0)
          .build();
    }

    final double capacityUnits = calculateWriteCapacityUnits(item);

    return ConsumedCapacity.builder()
        .tableName(tableName)
        .capacityUnits(capacityUnits)
        .writeCapacityUnits(capacityUnits)
        .table(Capacity.builder().capacityUnits(capacityUnits).writeCapacityUnits(capacityUnits).build())
        .build();
  }

  /**
   * Calculate read capacity units for an item.
   * DynamoDB charges 1 RCU per 4 KB, rounded up.
   *
   * @param item the item
   * @return the read capacity units
   */
  private double calculateReadCapacityUnits(final Map<String, AttributeValue> item) {
    final int itemSizeBytes = getItemSizeBytes(item);
    return Math.ceil((double) itemSizeBytes / READ_CAPACITY_UNIT_SIZE);
  }

  /**
   * Calculate write capacity units for an item.
   * DynamoDB charges 1 WCU per 1 KB, rounded up.
   *
   * @param item the item
   * @return the write capacity units
   */
  private double calculateWriteCapacityUnits(final Map<String, AttributeValue> item) {
    final int itemSizeBytes = getItemSizeBytes(item);
    return Math.ceil((double) itemSizeBytes / WRITE_CAPACITY_UNIT_SIZE);
  }

  /**
   * Get the size of an item in bytes by converting to JSON.
   *
   * @param item the item
   * @return the size in bytes
   */
  private int getItemSizeBytes(final Map<String, AttributeValue> item) {
    final String json = converter.toJson(item);
    return json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
  }
}
