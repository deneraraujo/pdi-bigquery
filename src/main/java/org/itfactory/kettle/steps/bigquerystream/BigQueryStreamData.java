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

import java.util.Map;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;

/**
 * Runtime transient data container for the PDI BigQuery stream step
 * 
 * @author afowler
 * @since 06-11-2017
 */
public class BigQueryStreamData extends BaseStepData {
  public RowMetaInterface outputRowMeta;
  public RowMetaInterface inputRowMeta;

  // Stores table primary key rows
  public String[] pk;

  // Stores columns types
  public Map<String, String> typeMap;

  // Bigquery stream state variables
  public BigQuery bigquery;
  public TableId tableId;
  public BigQueryStreamBatch batch;

  // Add and Update counters
  public int insertCount;
  public int updateCount;

  /**
   * Default constructor
   */
  public BigQueryStreamData() {
    super();
  }
}
