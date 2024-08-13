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

import java.io.FileInputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.itfactory.kettle.steps.bigquerystream.BigQueryStreamData.Batch;
import org.itfactory.kettle.steps.bigquerystream.BigQueryStreamData.Batch.Action;
import org.itfactory.kettle.steps.bigquerystream.BigQueryStreamData.TableInfo.ColumnInfo;
import org.itfactory.kettle.steps.bigquerystream.BigQueryStreamData.TableInfo.ConnectionStatus;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.PrimaryKey;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.BigQuery.TableOption;

/**
 * BigQuery Stream loading Output Step
 *
 * @author afowler
 * @since 06-11-2017
 */
public class BigQueryStream extends BaseStep {
  private BigQueryStreamMeta meta;
  private BigQueryStreamData data;
  private int rowCount;
  private LocalDateTime startAt;

  /**
   * Standard constructor
   */
  public BigQueryStream(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    rowCount = 0;
    startAt = LocalDateTime.now();
  }

  /**
   * Processes a single row in the PDI stream
   */
  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
    meta = (BigQueryStreamMeta) smi;
    data = (BigQueryStreamData) sdi;
    Object[] r = getRow(); // get row, set busy!

    // After last row
    if (r == null) {
      // No more input to be expected. Send pending data...
      if (rowCount == 0) {
        logBasic(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.InputTableEmpty"));
      } else {
        sendData();
      }
      dispose();
      return false;
    }

    // Create BigQuery row
    createBigQueryRow(r);

    // If batch reached limit, send data
    if (data.batch.size() >= 500) {
      sendData();
    }

    rowCount++;
    return true;
  }

  /**
   * Initializes the data for the step (meta data and runtime data)
   */
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (BigQueryStreamMeta) smi;
    data = (BigQueryStreamData) sdi;

    // Table info
    if (meta.getTableInfo() == null) {
      logError(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorGettingTableInfo"));
      stopAll();
    }

    // BigQuery connection
    try {
      data.bigQuery = getConnection(meta.getUseContainerSecurity(), meta.getProjectId(), meta.getCredentialsPath());
    } catch (Exception e) {
      logError(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ConnectionError"), e);
      stopAll();
    }

    // try to get table info from dialog
    if (meta.getTableInfo() == null || meta.getTableInfo().count() == 0) {
      try {
        // try to get table info from previous step
        meta.setTableInfo(
            BigQueryStreamData.TableInfo.getPrevStepTableInfo(getTransMeta(), getStepname(), meta.getTableName()));
        if (meta.getTableInfo() == null || meta.getTableInfo().count() == 0) {
          // try to get table info from BigQuery
          meta.setTableInfo(
              BigQueryStreamData.TableInfo.requestTableInfo(data.bigQuery, meta.getProjectId(), meta.getDatasetName(),
                  meta.getTableName()));
          // give up
          if (meta.getTableInfo() == null || meta.getTableInfo().count() == 0) {
            logError(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorGettingTableInfo"));
            stopAll();
          }
        }
      } catch (Exception e) {
        logError(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorGettingTableInfo"));
        stopAll();
      }
    }

    getOrCreateDataSet();
    getOrCreateTable();

    // Initialize variables
    data.batch = new Batch();
    data.insertCount = 0;
    data.updateCount = 0;

    return super.init(smi, sdi);
  }

  /**
   * Mount the output row, based on input
   */
  private void createBigQueryRow(Object[] rowMeta) throws KettleStepException {
    // Create row for BigQuery
    Map<String, Object> rowContent = new HashMap<String, Object>();

    // Copy fields names and values to BigQuery field
    int i = 0;
    for (String fieldName : getInputRowMeta().getFieldNames()) {
      ColumnInfo columnInfo = meta.getTableInfo().getMap().get(fieldName);
      Object valueData = rowMeta[i];

      // Workaround for datetime fields
      if (valueData != null && columnInfo.getType() == StandardSQLTypeName.TIMESTAMP) {
        valueData = rowMeta[i].toString();
      }

      // Add field to row
      rowContent.put(fieldName, valueData);
      i++;
    }

    // Sets history column
    if (meta.getTableInfo().getHistoryColumn() != null) {
      rowContent.put(meta.getTableInfo().getHistoryColumn(), startAt.toString());
    }

    // Add row to temp batch
    data.batch.addRow(rowContent, Action.Temp);
  }

  /**
   * Clean up connection and environment
   */
  private void dispose() {
    data.batch = null;
    data.tableId = null;
    setOutputDone();
  }

  /**
   * Initializes data.bigquery property
   */
  public static BigQuery getConnection(boolean useContainerSecurity, String projectId, String credentialsPath)
      throws Exception {
    BigQueryOptions options;

    if (useContainerSecurity) {
      options = BigQueryOptions.newBuilder()
          .setProjectId(projectId)
          .build();
    } else {
      try {
        options = BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setCredentials(GoogleCredentials.fromStream(
                new FileInputStream(credentialsPath)))
            .build();
      } catch (Exception e) {
        options = null;
        throw e;
      }
    }

    return options != null ? options.getService() : null;
  }

  /**
   * Get dataset if exists. If doesn't, create it
   */
  private Dataset getOrCreateDataSet() {
    DatasetInfo datasetInfo = Dataset.of(meta.getDatasetName());
    Dataset dataset = data.bigQuery.getDataset(datasetInfo.getDatasetId());

    if (dataset == null) {
      logBasic(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.CreatingDataset"));
      dataset = data.bigQuery.create(datasetInfo);
      meta.getTableInfo().setStatus(ConnectionStatus.TableNotExists);
    }
    data.tableId = TableId.of(dataset.getDatasetId().getDataset(), meta.getTableName());
    return dataset;
  }

  /**
   * Get table if exists. If doesn't, create it
   */
  private Table getOrCreateTable() {
    Table table;
    switch (meta.getTableInfo().getStatus()) {
      case TableExists:
        table = data.bigQuery.getTable(data.tableId);
        if (table != null)
          return table;
        table = createTable(meta.getTableInfo());
        if (table != null)
          return table;
        logError(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorGettingTable"));
        stopAll();
        break;
      case TableNotExists:
        BigQueryStreamData.TableInfo tableInfo = meta.getTableInfo();
        if (tableInfo == null || tableInfo.getColumnsInfo() == null || tableInfo.getColumnsInfo().length == 0) {
          logError(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorGettingTableInfo"));
          stopAll();
          break;
        }
        table = createTable(tableInfo);
        BigQueryStreamData.TableInfo.setRefreshPending(getStepname(), true);
        if (table == null) {
          if (tableInfo.getStatus().equals(ConnectionStatus.TableExists)) {
            table = data.bigQuery.getTable(data.tableId);
            return table;
          }
          stopAll();
          break;
        }
        return table;
      default:
        logError(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorGettingTableInfo"));
        stopAll();
        break;
    }
    return null;
  }

  /**
   * Create table on BigQuery
   */
  private Table createTable(BigQueryStreamData.TableInfo tableInfo) {
    Table table;

    try {
      logBasic(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.CreateTable"));
      List<Field> fieldList = new ArrayList<Field>();
      BigQueryStreamData.TableInfo.ColumnInfo[] colsInfo = tableInfo.getColumnsInfo();

      for (int i = 0; i < colsInfo.length + (tableInfo.getHistoryColumn() != null ? 1 : 0); i++) {
        boolean isHistoryCol = i == colsInfo.length;
        BigQueryStreamData.TableInfo.ColumnInfo colInfo;

        if (!isHistoryCol) {
          colInfo = colsInfo[i];
        } else {
          colInfo = new ColumnInfo(tableInfo.getHistoryColumn(), StandardSQLTypeName.TIMESTAMP);
          colInfo.setNullable(false);
        }

        Field.Builder fBuilder = Field.newBuilder(colInfo.getName(), colInfo.getType());
        fBuilder.setMode(colInfo.isNullable() ? Field.Mode.NULLABLE : Field.Mode.REQUIRED);
        fBuilder.setDescription(isHistoryCol ? BigQueryStreamData.TableInfo.historyColumnId : null);
        fieldList.add(fBuilder.build());
      }

      Schema schema = Schema.of(fieldList);
      TableId tableId = TableId.of(meta.getDatasetName(), tableInfo.getName());

      if (tableInfo.getPk() != null) {
        PrimaryKey.Builder pkBuilder = PrimaryKey.newBuilder();
        pkBuilder.setColumns(Arrays.asList(tableInfo.getPk()));

        TableConstraints.Builder tcBuilder = TableConstraints.newBuilder();
        tcBuilder.setPrimaryKey(pkBuilder.build());

        TableInfo.Builder tBuilder = Table.newBuilder(tableId, StandardTableDefinition.of(schema));
        tBuilder.setTableConstraints(tcBuilder.build());

        TableOption option = TableOption.autodetectSchema(true);
        table = data.bigQuery.create(tBuilder.build(), option);
      } else {
        table = data.bigQuery.create(TableInfo.of(tableId, StandardTableDefinition.of(schema)));
      }
      logBasic(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.TableCreated"));
      tableInfo.setStatus(ConnectionStatus.TableExists);
      data.tableId = tableId;
      return table;
    } catch (Exception e) {
      if (e instanceof BigQueryException && e.getMessage() != null
          && e.getMessage().toLowerCase().contains("already exists")) {
        tableInfo.setStatus(ConnectionStatus.TableExists);
        logBasic(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.TableAlreadyExists"));
        return null;
      }
      logError(String.format(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorCreatingTable"), e));
      return null;
    }
  }

  /**
   * Check if record already exists on BigQuery
   */
  private void checkIfExists(Action actionOnExists) {
    Map<String, Object>[] pendingBatch = data.batch.getRows(Action.Temp);

    if (pendingBatch.length > 0) {
      String query = "";
      QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder("SELECT 1");

      for (int i = 0; i < pendingBatch.length; i++) {
        Map<String, Object> rowContent = pendingBatch[i];
        String mainSelect = "";
        String subquerySelect = "";
        String subqueryCondition = "";

        for (BigQueryStreamData.TableInfo.ColumnInfo colInfo : meta.getTableInfo().getColumnsInfo()) {
          // Put field on main query "select"
          mainSelect += (mainSelect.isEmpty() ? "SELECT COUNT(1) > 0 AS `bigquery_exists`, " : ", ")
              + "IF (COUNT(1) > 0, MIN(subQuery.`" + colInfo.getName() + "`), @" + colInfo.getName() + i + ") AS `"
              + colInfo.getName() + "`";

          // Put field on subquery "select"
          subquerySelect += (subquerySelect.isEmpty() ? "SELECT " : ", ") + "`" + colInfo.getName() + "`";

          // If field is pk or if table has no pk's, add field to subquery "where" clause
          if ((meta.getTableInfo().getPk() != null
              && Arrays.stream(meta.getTableInfo().getPk()).anyMatch(colInfo.getName()::equals))
              || meta.getTableInfo().getPk() == null) {
            subqueryCondition += (subqueryCondition.isEmpty() ? "WHERE " : " AND ") + "`" + colInfo.getName()
                + "` = @" + colInfo.getName() + i;
          }

          builder.addNamedParameter(colInfo.getName() + i,
              QueryParameterValue.of(rowContent.get(colInfo.getName()), colInfo.getType()));
        }

        query += (query.isEmpty() ? "" : " UNION ALL ") + mainSelect + " FROM (" + subquerySelect + " FROM `"
            + meta.getDatasetName() + "."
            + meta.getTableName() + "` " + subqueryCondition + " LIMIT 1) AS `subQuery`";
      }

      if (!query.isEmpty()) {
        query += ";";
        builder.setQuery(query);
        logBasic(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.SearchingExistingRows"));
        Iterable<FieldValueList> response = null;
        try {
          response = data.bigQuery.query(builder.build()).iterateAll();
        } catch (Exception e) {
          logError(String.format(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorExecutingQuery"), e));
        }

        if (response != null) {
          for (FieldValueList row : response) {
            Map<String, Object> rowContent = new HashMap<>();
            for (BigQueryStreamData.TableInfo.ColumnInfo colInfo : meta.getTableInfo().getColumnsInfo()) {
              String pdiType = BigQueryStreamData.standardSqlToPdiType(colInfo.getType());
              Object fieldValue = BigQueryStreamData.fieldValueToPdiType(row.get(colInfo.getName()), pdiType);
              rowContent.put(colInfo.getName(), fieldValue);
            }

            // If exists, set to update/ignore. If doesn't, set to insert
            Action action = row.get("bigquery_exists").getBooleanValue() ? actionOnExists : Action.Add;

            // Add row to batch and send data if reached limit
            data.batch.addRow(rowContent, action);
          }
        }
      }
    }
  }

  /**
   * Send data
   */
  private boolean sendData() {
    Action actionOnExists = meta.getUpdateIfExists() ? Action.Update
        : (meta.getSkipIfExists() ? Action.None : null);

    // If user set action on exists
    if (actionOnExists != null) {
      // Check if exists, and distributes temp data into appropriate batches
      checkIfExists(actionOnExists);
    } else {
      // Move temp data to Insert batch
      for (Map<String, Object> rowContent : data.batch.getRows(Action.Temp)) {
        data.batch.addRow(rowContent, Action.Add);
      }
    }

    // Send data to BigQuery
    bigQuery_add();
    bigQuery_update();

    // Reset batch
    data.batch = new Batch();

    // Update output count
    setLinesWritten(data.insertCount);
    setLinesUpdated(data.updateCount);
    setLinesOutput(data.insertCount + data.updateCount);

    return true;
  }

  /**
   * Insert data on BigQuery
   */
  private boolean bigQuery_add() {
    return bigQuery_add(0);
  }

  /**
   * Insert data on BigQuery
   */
  private boolean bigQuery_add(int attempt) {
    Map<String, Object>[] batch = data.batch.getRows(Action.Add);
    if (batch.length > 0) {
      InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(data.tableId);

      for (Map<String, Object> rowContent : batch) {
        builder.addRow(rowContent);
      }

      logBasic(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.WritingData"));

      try {
        InsertAllRequest request = builder.build();
        InsertAllResponse response = data.bigQuery.insertAll(request);

        if (response.hasErrors()) {
          // If any of the insertions failed, this lets you inspect the errors
          for (Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
            // Inspect row error
            logError(String.format(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorWritingRows"),
                entry.toString()));
          }
          setErrors(getErrors() + response.getInsertErrors().size());
          return false;
        } else {
          // Insert count
          data.insertCount += batch.length;
          logBasic(batch.length + " " + BigQueryStreamMeta.getMessage("BigQueryStream.Messages.RecordsInserted"));
          return true;
        }
      } catch (Exception e) {
        if (e instanceof BigQueryException && e.getMessage() != null
            && e.getMessage().toLowerCase().contains("not found")) {
          if (attempt < 5) {
            logBasic(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.FailedToInsert"));
            bigQuery_add(attempt + 1);
          } else {
            logError(String.format(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorInsertingData"), e));
          }
        } else {
          logError(String.format(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorInsertingData"), e));
        }
        setErrors(getErrors() + batch.length);
      }
    }
    return false;
  }

  /**
   * Update data on BigQuery
   */
  private boolean bigQuery_update() {
    Map<String, Object>[] batch = data.batch.getRows(Action.Update);

    if (batch.length > 0 && meta.getTableInfo().getPk() != null) {
      String cmd = "";
      QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder("SELECT 1");

      for (int i = 0; i < batch.length; i++) {
        Map<String, Object> rowContent = batch[i];
        String cmdValues = "";
        String cmdCondition = "";

        for (BigQueryStreamData.TableInfo.ColumnInfo colInfo : meta.getTableInfo().getColumnsInfo()) {
          if (Arrays.stream(meta.getTableInfo().getPk()).anyMatch(colInfo.getName()::equals)) {
            // If field is pk, put on "where" clause
            cmdCondition += (cmdCondition.isEmpty() ? "WHERE " : " AND ") + "`" + colInfo.getName() + "` = @"
                + colInfo.getName() + i;
          } else {
            // If it's not, put on "set"
            cmdValues += (cmdValues.isEmpty() ? "SET " : ", ") + "`" + colInfo.getName() + "` = @" + colInfo.getName()
                + i;
          }
          builder.addNamedParameter(colInfo.getName() + i,
              QueryParameterValue.of(rowContent.get(colInfo.getName()), colInfo.getType()));
        }

        cmd += "UPDATE `" + meta.getDatasetName() + "." + meta.getTableName() + "` " + cmdValues + " "
            + cmdCondition + "; ";
      }

      if (!cmd.isEmpty()) {
        builder.setQuery(cmd);
        logBasic(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.WritingData"));
        Iterable<FieldValueList> response = null;

        try {
          response = data.bigQuery.query(builder.build()).iterateAll();
        } catch (Exception e) {
          if (e.getMessage().contains("affect rows in the streaming buffer")) {
            logError(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorBufferInUse"));
          } else {
            logError(String.format(BigQueryStreamMeta.getMessage("BigQueryStream.Messages.ErrorExecutingQuery"), e));
          }
        }

        if (response != null) {
          // Update count
          data.updateCount += batch.length;
          logBasic(batch.length + " " + BigQueryStreamMeta.getMessage("BigQueryStream.Messages.RecordsUpdated"));
          return true;
        }
      }
      setErrors(getErrors() + batch.length);
      return false;
    }
    return false;
  }
}
