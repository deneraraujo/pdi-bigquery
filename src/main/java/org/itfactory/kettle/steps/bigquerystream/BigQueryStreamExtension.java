package org.itfactory.kettle.steps.bigquerystream;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

/**
 * Support methods for BigQueryStream class
 *
 * @author deneraraujo
 * @since 08-07-2024
 */
public class BigQueryStreamExtension extends BaseStep {
    protected BigQueryStreamMeta meta;
    protected BigQueryStreamData data;

    /**
     * Standard constructor
     */
    public BigQueryStreamExtension(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * Initialisation on first row
     * 
     * @throws KettleStepException
     */
    protected void initialisation() throws KettleStepException {
        data.outputRowMeta = getInputRowMeta().clone();
        meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);
        getConnection();
        getOrCreateDataSet();
        getOrCreateTable();
        getTablePK();
        getTypeMap();

        // Initialise new builder
        data.batch = new BigQueryStreamBatch();
        data.insertCount = 0;
        data.updateCount = 0;

        first = false;
    }

    /**
     * Mount the output row, based on input
     */
    protected void createOutputRow(Object[] rowMeta) throws KettleStepException {
        // Create row for BigQuery
        data.inputRowMeta = getInputRowMeta();
        Map<String, Object> rowContent = new HashMap<String, Object>();

        // Copy fields names and values to BigQuery field
        int i = 0;
        for (String fieldName : data.inputRowMeta.getFieldNames()) {
            Object valueData = rowMeta[i];
            String type = data.typeMap.get(fieldName);

            // Workaround for datetime fields
            if (valueData != null && type == "Timestamp") {
                valueData = rowMeta[i].toString();
            }

            // Add field to row
            rowContent.put(fieldName, valueData);
            i++;
        }

        // Add row to temp batch
        data.batch.addRow(rowContent, BigQueryStreamBatch.Action.Temp);
    }

    /**
     * Clean up connection and environment
     */
    protected void dispose() {
        data.batch = null;
        data.tableId = null;
        setOutputDone();
    }

    /**
     * Initialises data.bigquery property
     */
    protected BigQuery getConnection() {
        BigQuery bigQuery;
        BigQueryOptions options;

        if (meta.getUseContainerSecurity()) {
            options = BigQueryOptions.newBuilder()
                    .setProjectId(meta.getProjectId())
                    .build();
        } else {
            try {
                options = BigQueryOptions.newBuilder()
                        .setProjectId(meta.getProjectId())
                        .setCredentials(GoogleCredentials.fromStream(
                                new FileInputStream(meta.getCredentialsPath())))
                        .build();
            } catch (IOException ioe) {
                options = null;
                logError("Error loading Google Credentials File", ioe);
                stopAll();
            }
        }

        bigQuery = options != null ? options.getService() : null;
        data.bigquery = bigQuery;
        return bigQuery;
    }

    /**
     * Get dataset if exists. If doesn't, create it
     */
    protected Dataset getOrCreateDataSet() {
        DatasetInfo datasetInfo = Dataset.of(meta.getDatasetName());
        Dataset dataset = data.bigquery.getDataset(datasetInfo.getDatasetId());

        if (dataset == null) {
            if (meta.getCreateDataset()) {
                logBasic("Creating dataset");
                dataset = data.bigquery.create(datasetInfo);
            } else {
                logBasic("Please create the dataset: " + meta.getDatasetName());
                System.exit(1);
            }
        }

        data.tableId = TableId.of(dataset.getDatasetId().getDataset(), meta.getTableName());
        return dataset;
    }

    /**
     * Get table if exists. If doesn't, create it
     */
    protected Table getOrCreateTable() {
        Table table = data.bigquery.getTable(data.tableId);
        if (table == null) {
            if (meta.getCreateTable()) {
                logBasic("Creating table");
                List<Field> fieldList = new ArrayList<Field>();

                // sample age data csv

                // TODO copy fields and types from input stream

                fieldList.add(Field.of("Name", LegacySQLTypeName.STRING));
                fieldList.add(Field.of("Age", LegacySQLTypeName.INTEGER));

                Schema schema = Schema.of(fieldList);
                table = data.bigquery.create(TableInfo.of(data.tableId, StandardTableDefinition.of(schema)));
            } else {
                logBasic("Please create the table: " + meta.getTableName());
                System.exit(1);
            }
        }

        return table;
    }

    /**
     * Get table primary key columns names
     */
    protected String[] getTablePK() {
        String[] pk = null;

        String query = "SELECT column_name FROM `" + meta.getProjectId() + "." + meta.getDatasetName() +
                "`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE `table_name` = '" + meta.getTableName() + "';";
        Iterable<FieldValueList> response = bigQuery_dml(query, null);

        if (response != null) {
            List<String> list = new ArrayList<String>();

            for (FieldValueList row : response) {
                list.add(row.get("column_name").getStringValue());
            }

            if (list.size() > 0) {
                pk = list.toArray(new String[list.size()]);
            }
        }

        data.pk = pk;
        return pk;
    }

    /**
     * Map columns types
     */
    protected Map<String, String> getTypeMap() {
        Map<String, String> typeMap = new HashMap<>();

        for (int i = 0; i < getInputRowMeta().size(); i++) {
            ValueMetaInterface v = getInputRowMeta().getValueMeta(i);
            typeMap.put(v.getName(), v.getTypeDesc());
        }

        data.typeMap = typeMap;
        return typeMap;
    }

    /**
     * Check if record already exists on BigQuery
     */
    protected void checkIfExists(BigQueryStreamBatch.Action actionOnExists) {
        Map<String, Object>[] pendingBatch = data.batch.getRows(BigQueryStreamBatch.Action.Temp);

        if (pendingBatch.length > 0) {
            String query = "";
            Map<String, QueryParameterValue> params = new HashMap<>();

            for (int i = 0; i < pendingBatch.length; i++) {
                Map<String, Object> rowContent = pendingBatch[i];
                String mainSelect = "";
                String subquerySelect = "";
                String subqueryCondition = "";

                for (String fieldName : data.inputRowMeta.getFieldNames()) {
                    String fieldType = data.typeMap.get(fieldName);
                    Object fieldValue = rowContent.get(fieldName);

                    // Put field on main query "select"
                    mainSelect += (mainSelect.isEmpty() ? "SELECT COUNT(1) > 0 AS `bigquery_exists`, " : ", ")
                            + "IF (COUNT(1) > 0, MIN(subQuery.`" + fieldName + "`), @" + fieldName + i + ") AS `"
                            + fieldName + "`";

                    // Put field on subquery "select"
                    subquerySelect += (subquerySelect.isEmpty() ? "SELECT " : ", ") + "`" + fieldName + "`";

                    // If field is pk or if table has no pk's, add field to subquery "where" clause
                    if ((data.pk != null && Arrays.stream(data.pk).anyMatch(fieldName::equals)) || data.pk == null) {
                        subqueryCondition += (subqueryCondition.isEmpty() ? "WHERE " : " AND ") + "`" + fieldName
                                + "` = @" + fieldName + i;
                    }
                    QueryParameterValue param = QueryParameterValue.of(stringToPdiType(fieldValue, fieldType),
                            pdiTypeToStandardSql(fieldType));
                    params.put(fieldName + i, param);
                }

                query += (query.isEmpty() ? "" : " UNION ALL ") + mainSelect + " FROM (" + subquerySelect + " FROM `"
                        + meta.getDatasetName() + "."
                        + meta.getTableName() + "` " + subqueryCondition + " LIMIT 1) AS `subQuery`";
            }

            if (!query.isEmpty()) {
                query += ";";
                logBasic("Searching for existing rows on BigQuery...");
                Iterable<FieldValueList> response = bigQuery_dml(query, params);

                if (response != null) {
                    for (FieldValueList row : response) {
                        Map<String, Object> rowContent = new HashMap<>();
                        for (String fieldName : data.inputRowMeta.getFieldNames()) {
                            rowContent.put(fieldName, row.get(fieldName).getValue());
                        }

                        // If exists, set to update/ignore. If doesn't, set to insert
                        BigQueryStreamBatch.Action action = row.get("bigquery_exists").getBooleanValue()
                                ? actionOnExists
                                : BigQueryStreamBatch.Action.Add;

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
    protected Boolean sendData() {
        BigQueryStreamBatch.Action actionOnExists = meta.getUpdateIfExists() ? BigQueryStreamBatch.Action.Update
                : (meta.getSkipIfExists() ? BigQueryStreamBatch.Action.None : null);

        // If user set action on exists
        if (actionOnExists != null) {
            // Check if exists, and distributes temp data into appropriate batches
            checkIfExists(actionOnExists);
        } else {
            // Move temp data to Insert batch
            for (Map<String, Object> rowContent : data.batch.getRows(BigQueryStreamBatch.Action.Temp)) {
                data.batch.addRow(rowContent, BigQueryStreamBatch.Action.Add);
            }
        }

        // Send data to BigQuery
        bigQuery_add();
        bigQuery_update();

        // Reset batch
        data.batch = new BigQueryStreamBatch();

        // Update output count
        setLinesWritten(data.insertCount);
        setLinesUpdated(data.updateCount);
        setLinesOutput(data.insertCount + data.updateCount);

        return true;
    }

    /**
     * Insert data on BigQuery
     */
    private Boolean bigQuery_add() {
        Map<String, Object>[] batch = data.batch.getRows(BigQueryStreamBatch.Action.Add);
        if (batch.length > 0) {
            InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(data.tableId);

            for (Map<String, Object> rowContent : batch) {
                builder.addRow(rowContent);
            }

            logBasic("Writing data to BigQuery...");
            InsertAllRequest request = builder.build();
            InsertAllResponse response = data.bigquery.insertAll(request);

            if (response.hasErrors()) {
                // If any of the insertions failed, this lets you inspect the errors
                for (Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    // Inspect row error
                    logError("Error writing rows to BigQuery: " + entry.toString());
                }
                return false;
            } else {
                // Insert count
                data.insertCount += batch.length;
                logBasic(batch.length + " records inserted.");
                return true;
            }
        }

        return false;
    }

    /**
     * Update data on BigQuery
     */
    private Boolean bigQuery_update() {
        Map<String, Object>[] batch = data.batch.getRows(BigQueryStreamBatch.Action.Update);

        if (batch.length > 0 && data.pk != null) {
            String cmd = "";
            Map<String, QueryParameterValue> params = new HashMap<>();

            for (int i = 0; i < batch.length; i++) {
                Map<String, Object> rowContent = batch[i];
                String cmdValues = "";
                String cmdCondition = "";

                for (String fieldName : data.inputRowMeta.getFieldNames()) {
                    String fieldType = data.typeMap.get(fieldName);
                    Object fieldValue = rowContent.get(fieldName);

                    if (Arrays.stream(data.pk).anyMatch(fieldName::equals)) {
                        // If field is pk, put on "where" clause
                        cmdCondition += (cmdCondition.isEmpty() ? "WHERE " : " AND ") + "`" + fieldName + "` = @"
                                + fieldName + i;
                    } else {
                        // If it's not, put on "set"
                        cmdValues += (cmdValues.isEmpty() ? "SET " : ", ") + "`" + fieldName + "` = @" + fieldName
                                + i;
                    }

                    QueryParameterValue param = QueryParameterValue.of(stringToPdiType(fieldValue, fieldType),
                            pdiTypeToStandardSql(fieldType));
                    params.put(fieldName + i, param);
                }

                cmd += "UPDATE `" + meta.getDatasetName() + "." + meta.getTableName() + "` " + cmdValues + " "
                        + cmdCondition + "; ";
            }

            if (!cmd.isEmpty()) {
                logBasic("Writing data to BigQuery...");
                Iterable<FieldValueList> response = bigQuery_dml(cmd, params);

                if (response != null) {
                    // Update count
                    data.updateCount += batch.length;
                    logBasic(batch.length + " records updated.");
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    /**
     * Execute a DML command to BigQuery (Data Manipulation Language)
     */
    private Iterable<FieldValueList> bigQuery_dml(String query, Map<String, QueryParameterValue> params) {
        Iterable<FieldValueList> response = null;
        QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(query);

        if (params != null) {
            for (Map.Entry<String, QueryParameterValue> param : params.entrySet()) {
                builder.addNamedParameter(param.getKey(), param.getValue());
            }
        }

        try {
            response = data.bigquery.query(builder.build()).iterateAll();
        } catch (Exception e) {
            if (e.getMessage().contains("affect rows in the streaming buffer")) {
                logError(
                        "Error on updating data on BigQuery due to streaming buffer in use. Wait approximately 90 minutes and try again.");
            } else {
                logError("Error executing query: ", e);
            }
        }

        return response;
    }

    /**
     * Check if value is String and convert to PDI type
     */
    private Object stringToPdiType(Object value, String pdiTypeDesc) {
        if (value instanceof String) {
            switch (pdiTypeDesc) {
                case "BigNumber":
                    value = new BigDecimal(value.toString());
                    break;
                case "Binary":
                    value = (value.toString()).getBytes();
                    break;
                case "Boolean":
                    value = Boolean.valueOf(value.toString());
                    break;
                case "Date":
                    break;
                case "Integer":
                    value = Integer.valueOf(value.toString());
                    break;
                case "Internet Address":
                    break;
                case "Number":
                    value = Double.valueOf((String) value);
                    break;
                case "String":
                    break;
                case "Timestamp":
                    break;
                default:
                    break;
            }
        }
        return value;
    }

    /**
     * Convert PDI type to Standad SQL type
     */
    private StandardSQLTypeName pdiTypeToStandardSql(String pdiTypeDesc) {
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
}
