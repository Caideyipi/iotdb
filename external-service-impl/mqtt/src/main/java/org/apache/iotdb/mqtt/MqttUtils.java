package org.apache.iotdb.mqtt;

import org.apache.iotdb.rpc.RpcUtils;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MqttUtils {

  /**
   * Construct a Tablet from multiple TableMessages. All messages should belong to the same table.
   * Parses TableMessages into row-based format and delegates to the existing method.
   *
   * @param messages list of TableMessages to convert
   * @return Tablet containing all data from messages
   */
  public static Tablet constructTabletFromMultipleMessages(List<TableMessage> messages) {
    if (messages.isEmpty()) {
      throw new IllegalArgumentException("Messages list is empty");
    }

    String tableName = messages.get(0).getTable();
    List<Long> times = new ArrayList<>(messages.size());
    List<List<String>> measurementsList = new ArrayList<>(messages.size());
    List<List<TSDataType>> typesList = new ArrayList<>(messages.size());
    List<List<ColumnCategory>> columnCategoriesList = new ArrayList<>(messages.size());
    List<List<Object>> valuesList = new ArrayList<>(messages.size());

    for (TableMessage message : messages) {
      // Timestamp
      times.add(message.getTimestamp());

      int fieldCount = message.getFields().size();
      int tagCount = message.getTagKeys().size();
      int attrCount = message.getAttributeKeys().size();

      // Measurements: fields + tags + attributes
      List<String> measurements =
          Stream.of(message.getFields(), message.getTagKeys(), message.getAttributeKeys())
              .flatMap(List::stream)
              .collect(Collectors.toList());
      measurementsList.add(measurements);

      // Types: field types + STRING for tags + STRING for attributes
      List<TSDataType> types = new ArrayList<>(measurements.size());
      types.addAll(message.getDataTypes());
      for (int i = 0; i < tagCount; i++) {
        types.add(TSDataType.STRING);
      }
      for (int i = 0; i < attrCount; i++) {
        types.add(TSDataType.STRING);
      }
      typesList.add(types);

      // Column categories: FIELD for fields, TAG for tags, ATTRIBUTE for attributes
      List<ColumnCategory> categories = new ArrayList<>(measurements.size());
      for (int i = 0; i < fieldCount; i++) {
        categories.add(ColumnCategory.FIELD);
      }
      for (int i = 0; i < tagCount; i++) {
        categories.add(ColumnCategory.TAG);
      }
      for (int i = 0; i < attrCount; i++) {
        categories.add(ColumnCategory.ATTRIBUTE);
      }
      columnCategoriesList.add(categories);

      // Values: fields + tags + attributes
      List<Object> values = new ArrayList<>(measurements.size());
      values.addAll(message.getValues());
      values.addAll(message.getTagValues());
      values.addAll(message.getAttributeValues());
      valuesList.add(values);
    }
    return RpcUtils.constructTabletFromMutipleRecords(
        tableName, times, measurementsList, typesList, columnCategoriesList, valuesList);
  }
}
