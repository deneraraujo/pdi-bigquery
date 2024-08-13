/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.itfactory.kettle.steps.bigquerystream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepMeta;
import org.w3c.dom.Node;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;

/**
 * Runtime transient data container for the PDI BigQuery stream step
 * 
 * @author afowler
 * @since 06-11-2017
 */
public class BigQueryStreamData extends BaseStepData {
  // Bigquery stream state variables
  public BigQuery bigQuery;
  public TableId tableId;
  public Batch batch;

  // Add and Update counters
  public int insertCount;
  public int updateCount;

  /**
   * Default constructor
   */
  public BigQueryStreamData() {
    super();
  }

  /**
   * Convert BigQuery FieldValue to correct java type, based on PDI types
   */
  public static Object fieldValueToPdiType(FieldValue value, String pdiTypeDesc) {
    if (value.getValue() != null) {
      switch (pdiTypeDesc) {
        case "BigNumber":
          return value.getNumericValue();
        case "Binary":
          return value.getBytesValue();
        case "Boolean":
          return value.getBooleanValue();
        case "Date":
          return value.getStringValue();
        case "Integer":
          return value.getLongValue();
        case "Internet Address":
          return value.getStringValue();
        case "Number":
          return value.getDoubleValue();
        case "String":
          return value.getStringValue();
        case "Timestamp":
          return value.getTimestampValue();
        default:
          return value.getStringValue();
      }
    }
    return value.getValue();
  }

  /**
   * Convert PDI type to Standad SQL type
   */
  public static StandardSQLTypeName pdiTypeToStandardSql(String pdiTypeDesc) {
    switch (pdiTypeDesc) {
      case "BigNumber":
        return StandardSQLTypeName.BIGNUMERIC;
      case "Binary":
        return StandardSQLTypeName.BYTES;
      case "Boolean":
        return StandardSQLTypeName.BOOL;
      case "Date":
        return StandardSQLTypeName.DATE;
      case "Integer":
        return StandardSQLTypeName.INT64;
      case "Internet Address":
        return StandardSQLTypeName.STRING;
      case "Number":
        return StandardSQLTypeName.NUMERIC;
      case "String":
        return StandardSQLTypeName.STRING;
      case "Timestamp":
        return StandardSQLTypeName.TIMESTAMP;
      default:
        return StandardSQLTypeName.STRING;
    }
  }

  /**
   * Convert Standard SQL type to PDI type
   */
  public static String standardSqlToPdiType(StandardSQLTypeName standardSqltype) {
    switch (standardSqltype) {
      case BIGNUMERIC:
        return "BigNumber";
      case BOOL:
        return "Boolean";
      case BYTES:
        return "Binary";
      case DATE:
        return "Date";
      case DATETIME:
        return "Date";
      case FLOAT64:
        return "Number";
      case INT64:
        return "Integer";
      case INTERVAL:
        return "Number";
      case NUMERIC:
        return "Number";
      case STRING:
        return "String";
      case TIME:
        return "Number";
      case TIMESTAMP:
        return "Timestamp";
      case ARRAY:
      case GEOGRAPHY:
      case JSON:
      case RANGE:
      case STRUCT:
      default:
        return "String";
    }
  }

  /**
   * Batch container for the PDI BigQuery stream step
   *
   * @author deneraraujo
   * @since 08-07-2024
   */
  public static class Batch {
    private ArrayList<Item> batch;

    /**
     * Initialize batch
     */
    public Batch() {
      batch = new ArrayList<>();
    }

    /**
     * Add row to a batch
     */
    public void addRow(Map<String, Object> rowContent, Action action) {
      batch.add(new Item(action, rowContent));
    }

    /**
     * Get batch total size
     */
    public int size() {
      return batch.size();
    }

    /**
     * Get rows from batch, filtered by action
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object>[] getRows(Action action) {
      if (batch.size() > 0) {
        ArrayList<Map<String, Object>> filtered = new ArrayList<Map<String, Object>>();
        for (Item item : batch) {
          if (item.action.equals(action))
            filtered.add(item.rowContent);
        }
        return filtered.toArray(new Map[0]);
      }
      return (Map<String, Object>[]) new Map[0];
    }

    /**
     * Action enum
     */
    public enum Action {
      Add, Update, None, Temp
    }

    /**
     * A batch item
     */
    private class Item {
      private Action action;
      private Map<String, Object> rowContent;

      public Item(Action action, Map<String, Object> rowContent) {
        this.action = action;
        this.rowContent = rowContent;
      }
    }
  }

  /**
   * Stores columns info
   */
  public static class TableInfo {
    private String name;
    private ArrayList<String> pk;
    private String historyColumn;
    private ConnectionStatus status;
    private String statusMessage;
    private Map<String, BigQueryStreamData.TableInfo.ColumnInfo> map;
    private ArrayList<BigQueryStreamData.TableInfo.ColumnInfo> list;
    private boolean sorted;
    public static final String historyColumnId = "BigQuery History Column";

    public TableInfo() {
      this(null);
    }

    public TableInfo(String name) {
      this(name, ConnectionStatus.TableNotExists,
          "BigQueryStream.Messages.TableDoesntExist");
    }

    public TableInfo(String name, ConnectionStatus status, String statusMessage) {
      this.name = name;
      pk = new ArrayList<>();
      historyColumn = null;
      map = new HashMap<>();
      list = new ArrayList<>();
      this.status = status;
      this.statusMessage = statusMessage;
      sorted = false;
    }

    public Map<String, BigQueryStreamData.TableInfo.ColumnInfo> getMap() {
      return map;
    }

    public ColumnInfo[] getColumnsInfo() {
      if (!sorted) {
        list.sort((o1, o2) -> o1.getPosition().compareTo(o2.getPosition()));
        sorted = true;
      }
      return list.toArray(new ColumnInfo[0]);
    }

    public String getName() {
      return name;
    }

    public void setName(String value) {
      name = value;
    }

    public String[] getPk() {
      if (pk.size() > 0) {
        return pk.toArray(new String[0]);
      }
      return null;
    }

    public String getHistoryColumn() {
      return historyColumn;
    }

    public void setHistoryColumn(String value) {
      historyColumn = value;
    }

    public ConnectionStatus getStatus() {
      return status;
    }

    public void setStatus(ConnectionStatus value) {
      status = value;
    }

    public String getStatusMessage() {
      return statusMessage;
    }

    public void setStatusMessage(String value) {
      statusMessage = value;
    }

    public void add(String name) {
      add(name, null);
    }

    public void add(String name, StandardSQLTypeName type) {
      add(name, type, 0, true, false, null);
    }

    public void add(String name, StandardSQLTypeName type, Integer position, Boolean isNullable, Boolean isPk,
        Integer pkPosition) {
      add(new ColumnInfo(name, type, position, isNullable, isPk, pkPosition));
    }

    public void add(ColumnInfo columnInfo) {
      list.add(columnInfo);
      map.put(columnInfo.name, columnInfo);
      if (columnInfo.isPk) {
        if (columnInfo.pkPosition != null) {
          pk.add(columnInfo.pkPosition, columnInfo.name);
        } else {
          pk.add(columnInfo.name);
        }
      }
      sorted = false;
    }

    public String getXML() {
      String xml = XMLHandler.openTag("tableInfo");
      xml += XMLHandler.addTagValue("name", name);
      xml += XMLHandler.addTagValue("historyColumn", historyColumn);
      xml += XMLHandler.addTagValue("status", status != null ? status.name() : null);
      xml += XMLHandler.addTagValue("statusMessage", statusMessage);
      for (BigQueryStreamData.TableInfo.ColumnInfo columnInfo : list) {
        xml += XMLHandler.openTag("columnInfo");
        xml += XMLHandler.addTagValue("name", columnInfo.name);
        xml += XMLHandler.addTagValue("type", columnInfo.type != null ? columnInfo.type.name() : null);
        xml += XMLHandler.addTagValue("position", columnInfo.position);
        xml += XMLHandler.addTagValue("isNullable",
            columnInfo.isNullable != null ? (columnInfo.isNullable ? "Y" : "N") : null);
        xml += XMLHandler.addTagValue("isPk", columnInfo.isPk != null ? (columnInfo.isPk ? "Y" : "N") : null);
        xml += XMLHandler.addTagValue("pkPosition",
            columnInfo.pkPosition != null ? columnInfo.pkPosition.toString() : null);
        xml += XMLHandler.closeTag("columnInfo");
      }
      xml += XMLHandler.closeTag("tableInfo");
      return xml;
    }

    public static TableInfo fromXml(String input) {
      try {
        return fromXml(XMLHandler.loadXMLString(input, "tableInfo"));
      } catch (Exception e) {
        return new TableInfo();
      }
    }

    public static TableInfo fromXml(Node xmlRoot) {
      try {
        String tableName = XMLHandler.getTagValue(xmlRoot, "name");
        String historyColumn = XMLHandler.getTagValue(xmlRoot, "historyColumn");
        String status = XMLHandler.getTagValue(xmlRoot, "status");
        String statusMessage = XMLHandler.getTagValue(xmlRoot, "statusMessage");
        List<Node> columnsInfo = XMLHandler.getNodes(xmlRoot, "columnInfo");

        TableInfo tableInfo = new TableInfo(tableName,
            status != null ? ConnectionStatus.valueOf(status) : null,
            statusMessage);

        if (!(Const.nullToEmpty(historyColumn).trim().isEmpty()))
          tableInfo.setHistoryColumn(historyColumn.trim());

        for (Node node : columnsInfo) {
          String colName = XMLHandler.getTagValue(node, "name");
          String type = XMLHandler.getTagValue(node, "type");
          String position = XMLHandler.getTagValue(node, "position");
          String isNullable = XMLHandler.getTagValue(node, "isNullable");
          String isPk = XMLHandler.getTagValue(node, "isPk");
          String pkPosition = XMLHandler.getTagValue(node, "pkPosition");

          tableInfo.add(colName,
              type != null ? StandardSQLTypeName.valueOf(type) : null,
              position != null ? Integer.valueOf(position) : null,
              isNullable != null ? isNullable.equals("Y") : null,
              isPk != null ? isPk.equals("Y") : null,
              pkPosition != null ? Integer.valueOf(pkPosition) : null);
        }
        return tableInfo;
      } catch (Exception e) {
        return new TableInfo();
      }
    }

    public int count() {
      return list == null ? 0 : list.size();
    }

    /**
     * Get fields info from previous step
     */
    public static TableInfo getPrevStepTableInfo(TransMeta transMeta, String stepName, String tableName) {
      try {
        StepMeta stepMeta = transMeta.getSteps().stream().filter(sm -> sm.getName().equals(stepName)).findFirst()
            .orElse(null);
        StepMeta[] prevs = transMeta.getPrevSteps(stepMeta);

        if (prevs.length > 0) {
          RowMetaInterface rowMeta = transMeta.getStepFields(prevs[0]);
          if (rowMeta != null) {
            String[] fieldNames = rowMeta.getFieldNames();
            TableInfo tableInfo = new TableInfo(tableName);

            for (int i = 0; i < fieldNames.length; i++) {
              String name = fieldNames[i];
              String type = BigQueryStreamData.pdiTypeToStandardSql(rowMeta.getValueMeta(i).getTypeDesc()).name();
              tableInfo.add(name, StandardSQLTypeName.valueOf(type));
            }
            return tableInfo;
          }
        }
      } catch (Exception e) {
        return null;
      }
      return null;
    }

    /**
     * Request table columns info from BigQuery
     */
    public static BigQueryStreamData.TableInfo requestTableInfo(BigQuery bigQuery, String projectId, String datasetName,
        String tableName)
        throws Exception {
      BigQueryStreamData.TableInfo tableInfo = null;

      if (bigQuery != null && tableName != null) {
        String query = "SELECT " +
            "`columns`.`column_name` AS `name`, " +
            "`columns`.`data_type` AS `type`, " +
            "`columns`.`ordinal_position` - 1 AS `position`, " +
            "`columns`.`is_nullable` = 'YES' AS `is_nullable`, " +
            "`keys`.`constraint_name` IS NOT NULL AS `is_pk`, " +
            "CASE WHEN `keys`.`ordinal_position` IS NULL THEN NULL ELSE `keys`.`ordinal_position` - 1 END AS `pk_position`, "
            + "`fieldPaths`.`description` AS `description`" +
            "FROM " +
            "`" + projectId + "." + datasetName + "`.INFORMATION_SCHEMA.COLUMNS AS `columns` " +
            "JOIN `" + projectId + "." + datasetName + "`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS AS `fieldPaths` " +
            "ON `fieldPaths`.`table_name` = `columns`.`table_name` AND `fieldPaths`.`column_name` = `columns`.`column_name` "
            + "LEFT JOIN `" + projectId + "." + datasetName + "`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS `keys` " +
            "ON `keys`.`constraint_name` = CONCAT(`columns`.`table_name`, '.', 'pk$') " +
            "AND `keys`.`table_name` = `columns`.`table_name` " +
            "AND `keys`.`column_name` = `columns`.`column_name` " +
            "WHERE " +
            "`columns`.`table_name` = @table_name " +
            "ORDER BY `columns`.`ordinal_position`;";

        Iterable<FieldValueList> response = null;
        try {
          QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(query);
          builder.addNamedParameter("table_name", QueryParameterValue.string(tableName));
          response = bigQuery.query(builder.build()).iterateAll();
        } catch (Exception e) {
          String msg = e != null && e.getMessage() != null ? e.getMessage().toLowerCase() : "";
          if (msg.contains("not found") && msg.contains("dataset")) {
            return new BigQueryStreamData.TableInfo(tableName, ConnectionStatus.TableNotExists,
                "BigQueryStream.Messages.DatasetDoesntExist");
          }
          throw e;
        }

        if (response != null) {
          tableInfo = new BigQueryStreamData.TableInfo(tableName);
          int i = 0;
          for (FieldValueList row : response) {
            String colName = row.get("name").getStringValue();
            boolean isHistoryCol = false;

            if (tableInfo.historyColumn == null && !row.get("description").isNull()) {
              String description = row.get("description").getStringValue();
              if (description.equals(historyColumnId)) {
                tableInfo.historyColumn = colName;
                isHistoryCol = true;
              }
            }
            if (!isHistoryCol) {
              tableInfo.add(colName,
                  StandardSQLTypeName.valueOf(row.get("type").getStringValue()),
                  row.get("position").getNumericValue().intValue(),
                  row.get("is_nullable").getBooleanValue(),
                  row.get("is_pk").getBooleanValue(),
                  row.get("pk_position").isNull() ? null : row.get("pk_position").getNumericValue().intValue());
            }
            i++;
          }

          if (i > 0) {
            tableInfo.setStatus(ConnectionStatus.TableExists);
            tableInfo.setStatusMessage("BigQueryStream.Messages.TableExists");
          } else {
            tableInfo.setStatus(ConnectionStatus.TableNotExists);
            tableInfo.setStatusMessage("BigQueryStream.Messages.TableDoesntExist");
          }
        }
      }
      return tableInfo;
    }

    public static class ColumnInfo {
      private String name;
      private StandardSQLTypeName type;
      private Integer position;
      private Boolean isNullable;
      private Boolean isPk;
      private Integer pkPosition;

      public ColumnInfo(String name, StandardSQLTypeName type) {
        this(name, type, 0, true, false, null);
      }

      public ColumnInfo(String name, StandardSQLTypeName type, Integer position, Boolean isNullable, Boolean isPk,
          Integer pkPosition) {
        this.name = name;
        this.type = type;
        this.position = position;
        this.isNullable = isNullable;
        this.isPk = isPk;
        this.pkPosition = pkPosition;
      }

      public String getName() {
        return name;
      }

      public StandardSQLTypeName getType() {
        return type;
      }

      public void setType(StandardSQLTypeName value) {
        type = value;
      }

      public Integer getPosition() {
        return position;
      }

      public void setPosition(Integer value) {
        position = value;
      }

      public Boolean isNullable() {
        return isNullable;
      }

      public void setNullable(boolean value) {
        isNullable = value;
      }

      public Boolean isPk() {
        return isPk;
      }

      public void setPk(boolean value) {
        isPk = value;
        if (!value) {
          pkPosition = null;
        }
      }

      public Integer getPkPosition() {
        return pkPosition;
      }

      public void setPkPosition(Integer value) {
        pkPosition = value;
        if (value == null) {
          isPk = false;
        }
      }
    }

    public static enum ConnectionStatus {
      Error,
      Loading,
      TableExists,
      TableNotExists
    }

    public static String getAppDataPath() {
      String os = System.getProperty("os.name").toLowerCase();
      String path = "";
      if (os.indexOf("win") >= 0) {
        path = System.getenv("AppData");
      } else {
        path = System.getProperty("user.home");
        if (os.indexOf("mac") >= 0) {
          path += "/Library/Application Support";
        }
      }
      path += "/.pdi-bigquery";
      return path;
    }

    public static void setRefreshPending(String stepName, boolean pending) {
      List<String> steps = new ArrayList<>();
      String appDataPath = getAppDataPath();
      Path appDataPathPath = Paths.get(appDataPath);
      Path filePath = Paths.get(appDataPath + "/pendingRefresh");
      try {
        if (!Files.exists(appDataPathPath)) {
          Files.createDirectories(appDataPathPath);
        } else if (Files.exists(filePath)) {
          steps = Files.readAllLines(filePath);
        }
        for (int i = 0; i < steps.size(); i++) {
          if (steps.get(i).contains(stepName + "=")) {
            steps.remove(i);
          }
        }
        steps.add(stepName + "=" + (pending ? "Y" : "N"));
        String content = String.join("\n", steps);

        Files.write(filePath, content.getBytes());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public static boolean getRefreshPending(String stepName) {
      String fileName = getAppDataPath() + "/pendingRefresh";
      try {
        Path path = Paths.get(fileName);
        List<String> steps = Files.readAllLines(path);
        for (int i = 0; i < steps.size(); i++) {
          if (steps.get(i).contains(stepName + "=")) {
            String[] pending = steps.get(i).split("=");
            return !pending[1].equals("N");
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return true;
    }
  }
}