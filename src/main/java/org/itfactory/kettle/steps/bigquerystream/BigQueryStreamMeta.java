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

import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;

import org.w3c.dom.Node;

@Step(id = "BigQueryStreamOutput", image = "google_query.svg", i18nPackageName = "org.itfactory.kettle.steps.bigquerystream", name = "BigQueryStream.Name", description = "BigQueryStream.Description", categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData")
/**
 * Metadata (configuration) holding class for the BigQuery stream loading custom
 * step
 * 
 * @author afowler
 * @since 06-11-2017
 */
public class BigQueryStreamMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = BigQueryStreamMeta.class; // for i18n purposes, needed by Translator2!!

  private boolean useContainerSecurity = false;
  private String credentialsPath = "";
  private String projectId = "";
  private String datasetName = "";
  private String tableName = "";
  private boolean skipIfExists = false;
  private boolean updateIfExists = false;
  private BigQueryStreamData.TableInfo tableInfo = new BigQueryStreamData.TableInfo();

  @Override
  /**
   * Loads step configuration from PDI ktr file XML
   */
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
    readData(stepnode);
  }

  @Override
  /**
   * Clones this meta class instance in PDI
   */
  public Object clone() {
    return super.clone();
  }

  @Override
  /**
   * Adds any additional fields to the stream
   */
  public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space) {
  }

  /**
   * Actually read the XML stream (used by loadXML())
   */
  private void readData(Node entrynode) throws KettleXMLException {
    try {
      String ucs = XMLHandler.getTagValue(entrynode, "useContainerSecurity");

      if (ucs == null || ucs.trim().isEmpty()) {
        useContainerSecurity = true;
      } else {
        useContainerSecurity = ucs.equals("Y");
      }
      projectId = XMLHandler.getTagValue(entrynode, "projectId");
      credentialsPath = XMLHandler.getTagValue(entrynode, "credentialsPath");
      datasetName = XMLHandler.getTagValue(entrynode, "datasetName");
      tableName = XMLHandler.getTagValue(entrynode, "tableName");

      String iie = XMLHandler.getTagValue(entrynode, "skipIfExists");
      if (iie == null || iie.trim().isEmpty()) {
        skipIfExists = true;
      } else {
        skipIfExists = iie.equals("Y");
      }

      String uie = XMLHandler.getTagValue(entrynode, "updateIfExists");
      if (uie == null || uie.trim().isEmpty()) {
        updateIfExists = true;
      } else {
        updateIfExists = uie.equals("Y");
      }

      tableInfo = BigQueryStreamData.TableInfo.fromXml(XMLHandler.getSubNode(entrynode, "tableInfo"));
    } catch (Exception e) {
      throw new KettleXMLException(BaseMessages.getString(
          PKG, "BigQueryStreamMeta.Exception.UnableToLoadStepInfo"), e);
    }
  }

  @Override
  /**
   * Sets default metadata configuration
   */
  public void setDefault() {
  }

  @Override
  /**
   * Returns the XML configuration of this step for saving in a ktr file
   */
  public String getXML() {
    StringBuffer retval = new StringBuffer(300);
    retval.append("      ").append(XMLHandler.addTagValue("useContainerSecurity", useContainerSecurity ? "Y" : "N"));
    retval.append("      ").append(XMLHandler.addTagValue("projectId", projectId));
    retval.append("      ").append(XMLHandler.addTagValue("credentialsPath", credentialsPath));
    retval.append("      ").append(XMLHandler.addTagValue("datasetName", datasetName));
    retval.append("      ").append(XMLHandler.addTagValue("tableName", tableName));
    retval.append("      ").append(XMLHandler.addTagValue("skipIfExists", skipIfExists ? "Y" : "N"));
    retval.append("      ").append(XMLHandler.addTagValue("updateIfExists", updateIfExists ? "Y" : "N"));
    retval.append("      ").append(tableInfo.getXML());

    return retval.toString();
  }

  @Override
  /**
   * Reads the configuration of this step from a repository
   */
  public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
      throws KettleException {

    try {
      useContainerSecurity = rep.getJobEntryAttributeBoolean(id_step, "useContainerSecurity");
      projectId = rep.getJobEntryAttributeString(id_step, "projectId");
      credentialsPath = rep.getJobEntryAttributeString(id_step, "credentialsPath");
      datasetName = rep.getJobEntryAttributeString(id_step, "datasetName");
      tableName = rep.getJobEntryAttributeString(id_step, "tableName");
      skipIfExists = rep.getJobEntryAttributeBoolean(id_step, "skipIfExists");
      updateIfExists = rep.getJobEntryAttributeBoolean(id_step, "updateIfExists");
      tableInfo = BigQueryStreamData.TableInfo.fromXml(rep.getJobEntryAttributeString(id_step, "tableInfo"));
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(
          PKG, "FieldAnalysisMeta.Exception.UnexpectedErrorWhileReadingStepInfo"), e);
    }
  }

  @Override
  /**
   * Saves the configuration of this step to a repository
   */
  public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step)
      throws KettleException {
    try {
      rep.saveJobEntryAttribute(id_transformation, getObjectId(), "useContainerSecurity", useContainerSecurity);
      rep.saveJobEntryAttribute(id_transformation, getObjectId(), "projectId", projectId);
      rep.saveJobEntryAttribute(id_transformation, getObjectId(), "credentialsPath", credentialsPath);
      rep.saveJobEntryAttribute(id_transformation, getObjectId(), "datasetName", datasetName);
      rep.saveJobEntryAttribute(id_transformation, getObjectId(), "tableName", tableName);
      rep.saveJobEntryAttribute(id_transformation, getObjectId(), "skipIfExists", skipIfExists);
      rep.saveJobEntryAttribute(id_transformation, getObjectId(), "updateIfExists", updateIfExists);
      rep.saveJobEntryAttribute(id_transformation, getObjectId(), "tableInfo", tableInfo.getXML());
    } catch (KettleException e) {
      throw new KettleException(BaseMessages.getString(PKG, "FieldAnalysisMeta.Exception.UnableToSaveStepInfo")
          + id_step, e);
    }
  }

  @Override
  /**
   * Validates this step's configuration
   */
  public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
      RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
      Repository repository, IMetaStore metaStore) {
    CheckResult cr;
    if (input.length > 0) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FieldAnalysisMeta.CheckResult.StepReceiveInfo.DialogMessage"), stepMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FieldAnalysisMeta.CheckResult.NoInputReceived.DialogMessage"), stepMeta);
      remarks.add(cr);
    }
  }

  /**
   * Get a message string by key
   */
  public static String getMessage(String key) {
    return BaseMessages.getString(PKG, key);
  }

  @Override
  /**
   * Returns a new instance of this step
   */
  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
      TransMeta transMeta, Trans trans) {
    return new BigQueryStream(stepMeta, stepDataInterface, cnr, transMeta, trans);
  }

  @Override
  /**
   * Returns a new instance of step data
   */
  public StepDataInterface getStepData() {
    return new BigQueryStreamData();
  }

  /**
   * Returns the google bigquery dataset name
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * Sets the dataset name for google bigquery
   */
  public void setDatasetName(String dsn) {
    datasetName = dsn;
  }

  /**
   * Returns the google bigquery table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Sets the google bigquery table name
   */
  public void setTableName(String tn) {
    tableName = tn;
  }

  /**
   * Returns whether to ignore existing records rather than creating new
   * ones
   */
  public boolean getSkipIfExists() {
    return skipIfExists;
  }

  /**
   * Sets whether to ignore existing records rather than creating new ones
   */
  public void setSkipIfExists(boolean doit) {
    skipIfExists = doit;
  }

  /**
   * Returns whether to update existing records rather than creating new
   * ones
   */
  public boolean getUpdateIfExists() {
    return updateIfExists;
  }

  /**
   * Sets whether to update existing records rather than creating new ones
   */
  public void setUpdateIfExists(boolean doit) {
    updateIfExists = doit;
  }

  /**
   * Returns whether to use container security (true) or google cloud credentials
   * json files (false)
   */
  public boolean getUseContainerSecurity() {
    return useContainerSecurity;
  }

  /**
   * Sets whether to use container security (true) or google cloud credentials
   * json files (false)
   */
  public void setUseContainerSecurity(boolean use) {
    useContainerSecurity = use;
  }

  /**
   * Returns the google bigquery project id
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * Sets the google bigquery project id
   * 
   * Note: Not used if using container security (set by the container)
   */
  public void setProjectId(String pid) {
    projectId = pid;
  }

  /**
   * Returns the google cloud credentials json file path
   * 
   * Note: Only used if useContainerSecurity is false
   */
  public String getCredentialsPath() {
    return credentialsPath;
  }

  /**
   * Sets the google cloud credentials json file path
   * 
   * Note: Only used if useContainerSecurity is false
   */
  public void setCredentialsPath(String cp) {
    credentialsPath = cp;
  }

  /**
   * Returns table information
   */
  public BigQueryStreamData.TableInfo getTableInfo() {
    return tableInfo;
  }

  /**
   * Sets table information
   */
  public void setTableInfo(BigQueryStreamData.TableInfo doit) {
    tableInfo = doit;
  }
}