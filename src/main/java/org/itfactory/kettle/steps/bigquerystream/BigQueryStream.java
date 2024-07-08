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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * BigQuery Stream loading Output Step
 *
 * @author afowler
 * @since 06-11-2017
 */
public class BigQueryStream extends BigQueryStreamExtension {

  /**
   * Standard constructor
   */
  public BigQueryStream(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
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
      sendData();
      dispose();
      return false;
    }

    // If is first row (initialisation)
    if (first) {
      initialisation();
    }

    // Create BigQuery row
    createOutputRow(r);

    // If batch reached limit, send data
    if (data.batch.size() >= 500) {
      sendData();
    }

    return true;
  }

  /**
   * Initialises the data for the step (meta data and runtime data)
   */
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (BigQueryStreamMeta) smi;
    data = (BigQueryStreamData) sdi;
    return super.init(smi, sdi);
  }
}
