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

import org.pentaho.di.core.Const;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.StandardSQLTypeName;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.TableEditor;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.itfactory.kettle.steps.bigquerystream.BigQueryStreamData.TableInfo.ConnectionStatus;

/**
 * Dialog box for the BigQuery stream loading step
 * 
 * @author afowler
 * @since 06-11-2017
 */
public class BigQueryStreamDialog extends BaseStepDialog implements StepDialogInterface {
  private BigQueryStreamMeta input;
  private TransMeta transMeta;

  private Shell parent;
  private Display display;

  // Controls
  private Label stepName_label;
  private FormData stepName_label_formdata;
  private Text stepName_text;
  private FormData stepName_text_formdata;
  private Button useContainerAuth_checkbox;
  private FormData useContainerAuth_checkbox_formdata;
  private Label credentialsPath_label;
  private FormData credentialsPath_label_formdata;
  private Text credentialsPath_text;
  private FormData credentialsPath_text_formdata;
  private Button credentialsPath_button;
  private FileDialog credentialsPath_button_filedialog;
  private FormData credentialsPath_button_formdata;
  private Label projectId_label;
  private FormData projectId_label_formdata;
  private Text projectId_text;
  private FormData projectId_text_formdata;
  private Label datasetName_label;
  private FormData datasetName_label_formdata;
  private Text datasetName_text;
  private FormData datasetName_text_formdata;
  private Label tableName_label;
  private FormData tableName_label_formdata;
  private Text tableName_text;
  private FormData tableName_text_formdata;
  private Label status_label;
  private FormData status_label_formdata;
  private Table grid;
  private String[] grid_colNames;
  private TableColumn[] grid_tableColumns;
  private TableColumn grid_tableColumn;
  private FormData grid_formdata;
  private Control[] grid_controls;
  private Button historyCol_checkbox;
  private FormData historyCol_checkbox_formdata;
  private Text historyCol_text;
  private FormData historyCol_text_formdata;
  private Button checkIfExists_checkbox;
  private FormData checkIfExists_checkbox_formdata;
  private Button skipIfExists_checkbox;
  private FormData skipIfExists_checkbox_formdata;
  private Button updateIfExists_checkbox;
  private FormData updateIfExists_checkbox_formdata;
  private Button ok_button;
  private Button cancel_button;

  // Variables to control text change on focus out
  private String tmpCredentialsPath;
  private String tmpProjectId;
  private String tmpDatasetName;
  private String tmpTableName;

  private BigQuery bigQuery;
  private BigQueryStreamData.TableInfo prevStepTableInfo;
  private ConnectionStatus connectionStatus;
  private String statusMessageKey;
  private boolean loading;
  private final String historyColumnDefaultName = "BigQueryHistoryColumn";

  /**
   * Standard PDI dialog constructor
   */
  public BigQueryStreamDialog(Shell parent, Object in, TransMeta tr, String sname) {
    super(parent, (BaseStepMeta) in, tr, sname);
    input = (BigQueryStreamMeta) in;
    transMeta = tr;
  }

  /**
   * Initializes and displays the dialog box
   */
  public String open() {
    parent = getParent();
    display = parent.getDisplay();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN);
    shell.setLayout(formLayout);
    shell.setText(BigQueryStreamMeta.getMessage("BigQueryStream.Name"));
    shell.addShellListener(new ShellAdapter() {
      // Detect X or ALT-F4 or something that kills this window...
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });
    props.setLook(shell);
    setShellImage(shell, input);

    // Step name
    stepName_label = new Label(shell, SWT.RIGHT);
    stepName_label.setText(BigQueryStreamMeta.getMessage("BigQueryStream.Name.Label"));
    props.setLook(stepName_label);
    stepName_label_formdata = new FormData();
    stepName_label_formdata.left = new FormAttachment(0, Const.MARGIN);
    stepName_label_formdata.top = new FormAttachment(0, Const.MARGIN);
    stepName_label.setLayoutData(stepName_label_formdata);
    stepName_text = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    stepName_text.setToolTipText(BigQueryStreamMeta.getMessage("BigQueryStream.Name.Tooltip"));
    props.setLook(stepName_text);
    stepName_text.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
      }
    });
    stepName_text_formdata = new FormData();
    stepName_text_formdata.left = new FormAttachment(0, Const.MARGIN);
    stepName_text_formdata.right = new FormAttachment(100, -Const.MARGIN);
    stepName_text_formdata.top = new FormAttachment(stepName_label, 0);
    stepName_text.setLayoutData(stepName_text_formdata);

    // Use Google container authentication
    useContainerAuth_checkbox = new Button(shell, SWT.CHECK);
    useContainerAuth_checkbox.setText(BigQueryStreamMeta.getMessage("BigQueryStream.UseContainerAuth.Label"));
    props.setLook(useContainerAuth_checkbox);
    useContainerAuth_checkbox.addListener(SWT.Selection, new Listener() {
      @Override
      public void handleEvent(Event event) {
        requestTableInfo();
      }
    });
    useContainerAuth_checkbox.setToolTipText(BigQueryStreamMeta.getMessage("BigQueryStream.UseContainerAuth.Tooltip"));
    useContainerAuth_checkbox_formdata = new FormData();
    useContainerAuth_checkbox_formdata.left = new FormAttachment(0, Const.MARGIN);
    useContainerAuth_checkbox_formdata.right = new FormAttachment(100, -Const.MARGIN);
    useContainerAuth_checkbox_formdata.top = new FormAttachment(stepName_text, Const.MARGIN * 2);
    useContainerAuth_checkbox.setLayoutData(useContainerAuth_checkbox_formdata);

    // Google Cloud JSON credentials file
    credentialsPath_label = new Label(shell, SWT.RIGHT);
    credentialsPath_label.setText(BigQueryStreamMeta.getMessage("BigQueryStream.CredentialsPath.Label"));
    props.setLook(credentialsPath_label);
    credentialsPath_label_formdata = new FormData();
    credentialsPath_label_formdata.left = new FormAttachment(0, Const.MARGIN);
    credentialsPath_label_formdata.top = new FormAttachment(useContainerAuth_checkbox, Const.MARGIN * 2);
    credentialsPath_label.setLayoutData(credentialsPath_label_formdata);
    credentialsPath_text = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    credentialsPath_text.setToolTipText(BigQueryStreamMeta.getMessage("BigQueryStream.CredentialsPath.Tooltip"));
    props.setLook(credentialsPath_text);
    credentialsPath_text.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
      }
    });
    credentialsPath_text.addListener(SWT.FocusOut, new Listener() {
      @Override
      public void handleEvent(Event event) {
        // Refresh grid on focus out
        if (!tmpCredentialsPath.toLowerCase().equals(credentialsPath_text.getText().toLowerCase())) {
          validateCredentialsFile(credentialsPath_text.getText());
          tmpCredentialsPath = credentialsPath_text.getText();
          requestTableInfo();
        }
      }
    });
    credentialsPath_text_formdata = new FormData();
    credentialsPath_text_formdata.left = new FormAttachment(0, Const.MARGIN);
    credentialsPath_text_formdata.right = new FormAttachment(100, -((Const.MARGIN * 2) + 30));
    credentialsPath_text_formdata.top = new FormAttachment(credentialsPath_label, 0);
    credentialsPath_text.setLayoutData(credentialsPath_text_formdata);
    credentialsPath_button = new Button(shell, SWT.PUSH);
    credentialsPath_button.setText("...");
    credentialsPath_button_filedialog = new FileDialog(shell, SWT.OPEN);
    credentialsPath_button.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent event) {
        credentialsPath_button_filedialog
            .setFilterNames(new String[] {
                BigQueryStreamMeta.getMessage("BigQueryStream.CredentialsPath.FilterNames") + " (*.json)" });
        credentialsPath_button_filedialog.setFilterExtensions(new String[] { "*.json" });
        String fn = credentialsPath_button_filedialog.open();
        if (fn != null) {
          credentialsPath_text.setText(fn);
          validateCredentialsFile(fn);
          if (!tmpCredentialsPath.toLowerCase().equals(fn.toLowerCase())) {
            tmpCredentialsPath = fn;
            requestTableInfo();
          }
        }
      }
    });
    credentialsPath_button_formdata = new FormData();
    credentialsPath_button_formdata.left = new FormAttachment(credentialsPath_text, Const.MARGIN);
    credentialsPath_button_formdata.right = new FormAttachment(100, -Const.MARGIN);
    credentialsPath_button_formdata.top = new FormAttachment(credentialsPath_label, 0);
    credentialsPath_button.setLayoutData(credentialsPath_button_formdata);

    // Google Cloud project ID
    projectId_label = new Label(shell, SWT.RIGHT);
    projectId_label.setText(BigQueryStreamMeta.getMessage("BigQueryStream.Project.Label"));
    props.setLook(projectId_label);
    projectId_label_formdata = new FormData();
    projectId_label_formdata.left = new FormAttachment(0, Const.MARGIN);
    projectId_label_formdata.top = new FormAttachment(credentialsPath_text, Const.MARGIN * 2);
    projectId_label.setLayoutData(projectId_label_formdata);
    projectId_text = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    projectId_text.setToolTipText(BigQueryStreamMeta.getMessage("BigQueryStream.Project.Tooltip"));
    props.setLook(projectId_text);
    projectId_text.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
      }
    });
    projectId_text.addListener(SWT.FocusOut, new Listener() {
      @Override
      public void handleEvent(Event event) {
        // Refresh grid on focus out
        if (!tmpProjectId.equals(projectId_text.getText())) {
          tmpProjectId = projectId_text.getText();
          requestTableInfo();
        }
      }
    });
    projectId_text_formdata = new FormData();
    projectId_text_formdata.left = new FormAttachment(0, Const.MARGIN);
    projectId_text_formdata.right = new FormAttachment(100, -Const.MARGIN);
    projectId_text_formdata.top = new FormAttachment(projectId_label, 0);
    projectId_text.setLayoutData(projectId_text_formdata);

    // Google BigQuery dataset name
    datasetName_label = new Label(shell, SWT.RIGHT);
    datasetName_label.setText(BigQueryStreamMeta.getMessage("BigQueryStream.DataSet.Label"));
    props.setLook(datasetName_label);
    datasetName_label_formdata = new FormData();
    datasetName_label_formdata.left = new FormAttachment(0, Const.MARGIN);
    datasetName_label_formdata.top = new FormAttachment(projectId_text, Const.MARGIN * 2);
    datasetName_label.setLayoutData(datasetName_label_formdata);
    datasetName_text = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    datasetName_text.setToolTipText(BigQueryStreamMeta.getMessage("BigQueryStream.DataSet.Tooltip"));
    props.setLook(datasetName_text);
    datasetName_text.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
      }
    });
    datasetName_text.addListener(SWT.FocusOut, new Listener() {
      @Override
      public void handleEvent(Event event) {
        // Refresh grid on focus out
        if (!tmpDatasetName.equals(datasetName_text.getText())) {
          tmpDatasetName = datasetName_text.getText();
          requestTableInfo();
        }
      }
    });
    datasetName_text_formdata = new FormData();
    datasetName_text_formdata.left = new FormAttachment(0, Const.MARGIN);
    datasetName_text_formdata.right = new FormAttachment(100, -Const.MARGIN);
    datasetName_text_formdata.top = new FormAttachment(datasetName_label, 0);
    datasetName_text.setLayoutData(datasetName_text_formdata);

    // Google BigQuery table name
    tableName_label = new Label(shell, SWT.RIGHT);
    tableName_label.setText(BigQueryStreamMeta.getMessage("BigQueryStream.Table.Label"));
    props.setLook(tableName_label);
    tableName_label_formdata = new FormData();
    tableName_label_formdata.left = new FormAttachment(0, Const.MARGIN);
    tableName_label_formdata.top = new FormAttachment(datasetName_text, Const.MARGIN * 2);
    tableName_label.setLayoutData(tableName_label_formdata);
    tableName_text = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    tableName_text.setToolTipText(BigQueryStreamMeta.getMessage("BigQueryStream.Table.Tooltip"));
    props.setLook(tableName_text);
    tableName_text.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
      }
    });
    tableName_text.addListener(SWT.FocusOut, new Listener() {
      @Override
      public void handleEvent(Event event) {
        // Refresh grid on focus out
        if (!tmpTableName.equals(tableName_text.getText())) {
          tmpTableName = tableName_text.getText();
          requestTableInfo();
        }
      }
    });
    tableName_text_formdata = new FormData();
    tableName_text_formdata.left = new FormAttachment(0, Const.MARGIN);
    tableName_text_formdata.right = new FormAttachment(100, -Const.MARGIN);
    tableName_text_formdata.top = new FormAttachment(tableName_label, 0);
    tableName_text.setLayoutData(tableName_text_formdata);

    // Status
    status_label = new Label(shell, SWT.CENTER);
    props.setLook(status_label);
    status_label_formdata = new FormData();
    status_label_formdata.left = new FormAttachment(0, Const.MARGIN);
    status_label_formdata.right = new FormAttachment(100, -Const.MARGIN);
    status_label_formdata.top = new FormAttachment(tableName_text, Const.MARGIN * 3);
    status_label.setLayoutData(status_label_formdata);

    // Table definition
    grid_colNames = new String[] {
        BigQueryStreamMeta.getMessage("BigQueryStream.Grid.Col1"),
        BigQueryStreamMeta.getMessage("BigQueryStream.Grid.Col2"),
        BigQueryStreamMeta.getMessage("BigQueryStream.Grid.Col3"),
        BigQueryStreamMeta.getMessage("BigQueryStream.Grid.Col4") };
    grid = new Table(shell, SWT.MULTI | SWT.BORDER | SWT.V_SCROLL);
    grid.setLinesVisible(true);
    grid.setHeaderVisible(true);
    grid_tableColumns = new TableColumn[grid_colNames.length];
    for (int i = 0; i < grid_colNames.length; i++) {
      grid_tableColumn = new TableColumn(grid, SWT.NONE);
      grid_tableColumn.setText(grid_colNames[i]);
      grid_tableColumn.setResizable(false);
      grid_tableColumn.pack();
      grid_tableColumns[i] = grid_tableColumn;
    }
    props.setLook(grid);
    grid_formdata = new FormData();
    grid_formdata.left = new FormAttachment(0, Const.MARGIN);
    grid_formdata.right = new FormAttachment(100, -Const.MARGIN);
    grid_formdata.top = new FormAttachment(status_label, Const.MARGIN * 3);
    grid_formdata.height = 200;
    grid_formdata.width = 464;
    stepName_text_formdata.left = new FormAttachment(0, Const.MARGIN);
    grid.setLayoutData(grid_formdata);
    grid.addListener(SWT.EraseItem, new Listener() {
      // Workaround to disable cell highlight
      @Override
      public void handleEvent(Event event) {
        event.detail &= ~SWT.SELECTED;
        event.detail &= ~SWT.HOT;
      }
    });

    // History column
    historyCol_checkbox = new Button(shell, SWT.CHECK);
    historyCol_checkbox.setText(BigQueryStreamMeta.getMessage("BigQueryStream.HistoryCol.Label"));
    historyCol_checkbox.setSelection(true);
    props.setLook(historyCol_checkbox);
    historyCol_checkbox.setToolTipText(BigQueryStreamMeta.getMessage("BigQueryStream.HistoryCol.Tooltip"));
    historyCol_checkbox_formdata = new FormData();
    historyCol_checkbox_formdata.left = new FormAttachment(0, Const.MARGIN);
    historyCol_checkbox_formdata.top = new FormAttachment(grid, (int) (Const.MARGIN * 1.5));
    historyCol_checkbox.setLayoutData(historyCol_checkbox_formdata);
    historyCol_checkbox.addListener(SWT.Selection, new Listener() {
      @Override
      public void handleEvent(Event event) {
        handleHistoryCol();
      }
    });
    historyCol_text = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    historyCol_text_formdata = new FormData();
    historyCol_text_formdata.left = new FormAttachment(historyCol_checkbox, Const.MARGIN);
    historyCol_text_formdata.right = new FormAttachment(100, -Const.MARGIN);
    historyCol_text_formdata.top = new FormAttachment(grid, Const.MARGIN);
    historyCol_text.setLayoutData(historyCol_text_formdata);

    // Check for existing records
    checkIfExists_checkbox = new Button(shell, SWT.CHECK);
    checkIfExists_checkbox.setText(BigQueryStreamMeta.getMessage("BigQueryStream.CheckIfExists.Label"));
    checkIfExists_checkbox.setSelection(false);
    props.setLook(checkIfExists_checkbox);
    checkIfExists_checkbox.setToolTipText(BigQueryStreamMeta.getMessage("BigQueryStream.CheckIfExists.Tooltip"));
    checkIfExists_checkbox_formdata = new FormData();
    checkIfExists_checkbox_formdata.left = new FormAttachment(0, Const.MARGIN);
    checkIfExists_checkbox_formdata.right = new FormAttachment(100, -Const.MARGIN);
    checkIfExists_checkbox_formdata.top = new FormAttachment(historyCol_checkbox, Const.MARGIN * 2);
    checkIfExists_checkbox.setLayoutData(checkIfExists_checkbox_formdata);
    checkIfExists_checkbox.addListener(SWT.Selection, new Listener() {
      @Override
      public void handleEvent(Event event) {
        handleCheckIfExists();
      }
    });

    // Skip existing records
    skipIfExists_checkbox = new Button(shell, SWT.RADIO);
    skipIfExists_checkbox.setText(BigQueryStreamMeta.getMessage("BigQueryStream.SkipIfExists.Label"));
    props.setLook(skipIfExists_checkbox);
    skipIfExists_checkbox.setToolTipText(BigQueryStreamMeta.getMessage("BigQueryStream.SkipIfExists.Tooltip"));
    skipIfExists_checkbox_formdata = new FormData();
    skipIfExists_checkbox_formdata.left = new FormAttachment(0, Const.MARGIN * 5);
    skipIfExists_checkbox_formdata.right = new FormAttachment(100, -Const.MARGIN);
    skipIfExists_checkbox_formdata.top = new FormAttachment(checkIfExists_checkbox, Const.MARGIN);
    skipIfExists_checkbox.setLayoutData(skipIfExists_checkbox_formdata);

    // Update existing records
    updateIfExists_checkbox = new Button(shell, SWT.RADIO);
    updateIfExists_checkbox.setText(BigQueryStreamMeta.getMessage("BigQueryStream.UpdateIfExists.Label"));
    props.setLook(updateIfExists_checkbox);
    updateIfExists_checkbox.setToolTipText(BigQueryStreamMeta.getMessage("BigQueryStream.UpdateIfExists.Tooltip"));
    updateIfExists_checkbox_formdata = new FormData();
    updateIfExists_checkbox_formdata.left = new FormAttachment(0, Const.MARGIN * 5);
    updateIfExists_checkbox_formdata.right = new FormAttachment(100, -Const.MARGIN);
    updateIfExists_checkbox_formdata.top = new FormAttachment(skipIfExists_checkbox, Const.MARGIN);
    updateIfExists_checkbox.setLayoutData(updateIfExists_checkbox_formdata);

    ok_button = new Button(shell, SWT.PUSH);
    ok_button.setText(BigQueryStreamMeta.getMessage("System.Button.OK"));
    ok_button.addListener(SWT.Selection, new Listener() {
      public void handleEvent(Event e) {
        ok();
      }
    });

    cancel_button = new Button(shell, SWT.PUSH);
    cancel_button.setText(BigQueryStreamMeta.getMessage("System.Button.Cancel"));
    cancel_button.addListener(SWT.Selection, new Listener() {
      public void handleEvent(Event e) {
        cancel();
      }
    });

    BaseStepDialog.positionBottomButtons(shell, new Button[] { ok_button, cancel_button }, Const.MARGIN,
        updateIfExists_checkbox);

    new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    getData();

    shell.pack();
    Rectangle pSize = parent.getBounds();
    shell.setLocation((pSize.width - shell.getBounds().width) / 2, (pSize.height - shell.getBounds().height) / 2);
    parent.setEnabled(false);
    shell.open();
    props.setDialogSize(shell, "GoogleBigQueryStreamDialogSize");
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return stepname;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    stepName_text.setText(Const.nullToEmpty(stepname));
    useContainerAuth_checkbox.setSelection(input.getUseContainerSecurity());
    credentialsPath_text.setText(Const.nullToEmpty(input.getCredentialsPath()));
    validateCredentialsFile(credentialsPath_text.getText());
    projectId_text.setText(Const.nullToEmpty(input.getProjectId()));
    datasetName_text.setText(Const.nullToEmpty(input.getDatasetName()));
    tableName_text.setText(Const.nullToEmpty(input.getTableName()));
    checkIfExists_checkbox.setSelection(input.getSkipIfExists() || input.getUpdateIfExists());
    skipIfExists_checkbox.setSelection(input.getSkipIfExists());
    skipIfExists_checkbox.setEnabled(checkIfExists_checkbox.getSelection());
    updateIfExists_checkbox.setSelection(input.getUpdateIfExists());
    updateIfExists_checkbox.setEnabled(checkIfExists_checkbox.getSelection());
    stepName_text.selectAll();
    stepName_text.setFocus();

    tmpCredentialsPath = credentialsPath_text.getText();
    tmpProjectId = projectId_text.getText();
    tmpDatasetName = datasetName_text.getText();
    tmpTableName = tableName_text.getText();

    if (initGrid()) {
      if (!BigQueryStreamData.TableInfo.getRefreshPending(stepname)
          && checkPrevStep() && validateTableInfo(input.getTableInfo())) {
        updateGrid(input.getTableInfo());
      } else {
        requestTableInfo();
      }
    } else {
      setGridEnabled(false);
    }
  }

  /**
   * Validate table information, set status and enable/disable grid
   */
  private boolean validateTableInfo(BigQueryStreamData.TableInfo tableInfo) {
    if (tableInfo != null && tableInfo.getStatus() != null) {
      switch (tableInfo.getStatus()) {
        case TableExists:
          if (compareInputOutput(tableInfo)) {
            setConnectionStatus(tableInfo.getStatus(), tableInfo.getStatusMessage());
            setGridEnabled(true, false);
            return true;
          } else {
            setConnectionStatus(ConnectionStatus.Error, "BigQueryStream.Messages.ColumnsDoesntMatch");
            setGridEnabled(false);
            return false;
          }
        case TableNotExists:
          setConnectionStatus(tableInfo.getStatus(), tableInfo.getStatusMessage());
          setGridEnabled(true);
          return true;
        case Error:
          setConnectionStatus(tableInfo.getStatus(), tableInfo.getStatusMessage());
          setGridEnabled(false);
          return false;
        case Loading:
          setConnectionStatus(tableInfo.getStatus(), tableInfo.getStatusMessage());
          setGridEnabled(false);
          return false;
        default:
          setGridEnabled(false);
          return false;
      }
    } else {
      setGridEnabled(false);
      return false;
    }
  }

  /**
   * Check if input fields list is the same as what is in BigQuery
   */
  @SuppressWarnings("null")
  private boolean compareInputOutput(BigQueryStreamData.TableInfo output) {
    boolean zeroInput = prevStepTableInfo == null || prevStepTableInfo.getColumnsInfo() == null
        || prevStepTableInfo.count() == 0;
    boolean zeroOutput = output == null || output.getColumnsInfo() == null || output.count() == 0;

    if (zeroInput && zeroOutput)
      return true;

    if (zeroInput || zeroOutput)
      return false;

    if (prevStepTableInfo.count() != output.count())
      return false;

    for (int i = 0; i < output.count(); i++) {
      BigQueryStreamData.TableInfo.ColumnInfo inputCol = prevStepTableInfo.getColumnsInfo()[i];
      BigQueryStreamData.TableInfo.ColumnInfo outputCol = output.getMap().get(inputCol.getName());

      if (inputCol.getType() == null && outputCol.getType() != null)
        return false;

      if (inputCol.getType() != null && outputCol.getType() == null)
        return false;

      if (!inputCol.getType().equals(outputCol.getType()))
        return false;
    }

    return true;
  }

  /**
   * Check if credentials file exists
   */
  private boolean validateCredentialsFile(String input) {
    if (input == null) {
      credentialsPath_text.setForeground(new Color(255, 0, 0));
      return false;
    }
    File f = new File(input);
    if (!f.exists() || f.isDirectory()) {
      credentialsPath_text.setForeground(new Color(255, 0, 0));
      return false;
    }
    credentialsPath_text.setForeground(display.getSystemColor(SWT.COLOR_LIST_FOREGROUND));
    return true;
  }

  /**
   * Get table information from BigQuery
   */
  private boolean requestTableInfo() {
    boolean validation = checkPrevStep() && checkRequiredFields() && checkCredentials();
    if (validation) {
      setConnectionStatus(ConnectionStatus.Loading, "BigQueryStream.Messages.GettingTableInfo");
      setGridEnabled(false);
      ok_button.setEnabled(false);
      cancel_button.setEnabled(false);
      loading = true;
      display.asyncExec(new Runnable() {
        @Override
        public void run() {
          try {
            BigQueryStreamData.TableInfo tableInfo = BigQueryStreamData.TableInfo.requestTableInfo(bigQuery,
                projectId_text.getText(),
                datasetName_text.getText(),
                tableName_text.getText());

            if (validateTableInfo(tableInfo)) {
              updateGrid(tableInfo);
            }
          } catch (Exception e) {
            setConnectionStatus(ConnectionStatus.Error,
                "BigQueryStream.Messages.ErrorGettingTableInfo" + e.getMessage());
          }
          ok_button.setEnabled(true);
          cancel_button.setEnabled(true);
          loading = false;
        }
      });
    } else {
      setGridEnabled(false);
    }
    return validation;
  }

  /**
   * Check if previous step is connected to this
   */
  private boolean checkPrevStep() {
    if (prevStepTableInfo == null) {
      setConnectionStatus(ConnectionStatus.Error, "BigQueryStream.Messages.CantLoadInput");
      return false;
    }
    return true;
  }

  /**
   * Validate required fields
   */
  private boolean checkRequiredFields() {
    if (credentialsPath_text.getText().isEmpty()) {
      setConnectionStatus(ConnectionStatus.Error, "BigQueryStream.Messages.JsonRequired");
      return false;
    }
    if (projectId_text.getText().isEmpty()) {
      setConnectionStatus(ConnectionStatus.Error, "BigQueryStream.Messages.ProjectIdRequired");
      return false;
    }
    if (datasetName_text.getText().isEmpty()) {
      setConnectionStatus(ConnectionStatus.Error, "BigQueryStream.Messages.DatasetNameRequired");
      return false;
    }
    if (tableName_text.getText().isEmpty()) {
      setConnectionStatus(ConnectionStatus.Error, "BigQueryStream.Messages.TableNameRequired");
      return false;
    }
    return true;
  }

  /**
   * Load data to table definition grid
   */
  private boolean initGrid() {
    if (prevStepTableInfo == null) {
      prevStepTableInfo = BigQueryStreamData.TableInfo.getPrevStepTableInfo(transMeta, stepname,
          tableName_text.getText());
      if (!checkPrevStep())
        return false;
    }

    ArrayList<Control> controls = new ArrayList<>();
    for (BigQueryStreamData.TableInfo.ColumnInfo columnInfo : prevStepTableInfo.getColumnsInfo()) {
      TableItem item = new TableItem(grid, SWT.NONE);
      TableEditor editor;
      Label label;
      Button button;
      int colNr = 0;

      // Column name
      editor = new TableEditor(grid);
      label = new Label(grid, SWT.NONE);
      label.setText(columnInfo.getName());
      label.setToolTipText(columnInfo.getName());
      label.setBackground(display.getSystemColor(SWT.COLOR_LIST_BACKGROUND));
      label.setForeground(display.getSystemColor(SWT.COLOR_LIST_FOREGROUND));
      label.pack();
      editor.minimumWidth = label.getSize().x;
      editor.horizontalAlignment = SWT.LEFT;
      editor.setEditor(label, item, colNr);
      grid_tableColumns[colNr].setWidth(200);
      controls.add(label);
      colNr++;

      // Data type
      editor = new TableEditor(grid);
      label = new Label(grid, SWT.NONE);
      label.setText(columnInfo.getType() != null ? columnInfo.getType().name() : "");
      label.setBackground(display.getSystemColor(SWT.COLOR_LIST_BACKGROUND));
      label.setForeground(display.getSystemColor(SWT.COLOR_LIST_FOREGROUND));
      label.setSize(90 - (Const.MARGIN * 3), label.getSize().y);
      editor.minimumWidth = label.getSize().x;
      editor.horizontalAlignment = SWT.CENTER;
      editor.setEditor(label, item, colNr);
      grid_tableColumns[colNr].setWidth(90);
      controls.add(label);
      colNr++;

      // Is nullable
      editor = new TableEditor(grid);
      button = new Button(grid, SWT.CHECK);
      button.setSelection(true);
      button.pack();
      editor.minimumWidth = button.getSize().x;
      editor.horizontalAlignment = SWT.CENTER;
      editor.setEditor(button, item, colNr);
      grid_tableColumns[colNr].setWidth(86);
      controls.add(button);
      colNr++;

      // Is primary key
      editor = new TableEditor(grid);
      button = new Button(grid, SWT.CHECK);
      button.setSelection(false);
      button.pack();
      editor.minimumWidth = button.getSize().x;
      editor.horizontalAlignment = SWT.CENTER;
      editor.setEditor(button, item, colNr);
      grid_tableColumns[colNr].setWidth(86);
      controls.add(button);
      colNr++;
    }

    grid_controls = controls.toArray(new Control[0]);
    return true;
  }

  /**
   * Update table with columns information
   */
  private boolean updateGrid(BigQueryStreamData.TableInfo tableInfo) {
    boolean response = true;
    if (grid_controls != null && tableInfo != null) {
      Map<String, BigQueryStreamData.TableInfo.ColumnInfo> columnsInfo = tableInfo.getMap();
      for (int i = 0; i < (grid_controls.length / 4); i++) {
        Label columndName = (Label) grid_controls[i * 4 + 0];
        Label dataType = (Label) grid_controls[i * 4 + 1];
        Button isNullable = (Button) grid_controls[i * 4 + 2];
        Button isPk = (Button) grid_controls[i * 4 + 3];

        BigQueryStreamData.TableInfo.ColumnInfo columnInfo = columnsInfo.get(columndName.getText());
        if (columnInfo != null) {
          dataType.setText(columnInfo.getType() != null ? columnInfo.getType().name() : "");
          isNullable.setSelection(columnInfo.isNullable());
          isPk.setSelection(columnInfo.isPk());
        } else {
          isNullable.setSelection(true);
          isPk.setSelection(false);
          response = false;
        }
      }
      String historyColumn = Const.nullToEmpty(tableInfo.getHistoryColumn()).trim();

      if (historyColumn.isEmpty() && tableInfo.getStatus() == ConnectionStatus.TableNotExists) {
        historyColumn = !historyCol_text.getText().trim().isEmpty() ? historyCol_text.getText().trim()
            : historyColumnDefaultName;
      }

      historyCol_checkbox.setSelection(!historyColumn.isEmpty());
      historyCol_text.setText(historyColumn);
      handleHistoryCol();
    } else {
      response = false;
    }
    input.setTableInfo(tableInfo);
    return response;
  }

  /**
   * Get table information defined on the grid
   */
  private BigQueryStreamData.TableInfo getGridValues() {
    BigQueryStreamData.TableInfo info = new BigQueryStreamData.TableInfo(tableName_text.getText(), connectionStatus,
        statusMessageKey);
    if (grid_controls != null) {
      for (int i = 0; i < (grid_controls.length / grid_colNames.length); i++) {
        Label columndName = (Label) grid_controls[i * grid_colNames.length + 0];
        Label dataType = (Label) grid_controls[i * grid_colNames.length + 1];
        Button isNullable = (Button) grid_controls[i * grid_colNames.length + 2];
        Button isPk = (Button) grid_controls[i * grid_colNames.length + 3];

        StandardSQLTypeName typeName = StandardSQLTypeName.valueOf(dataType.getText());
        info.add(columndName.getText(), typeName, i, isNullable.getSelection(), isPk.getSelection(), null);
      }
    }
    if (historyCol_checkbox.getSelection() && !historyCol_text.getText().trim().isEmpty())
      info.setHistoryColumn(historyCol_text.getText().trim());
    return info;
  }

  /**
   * Output connection status
   */
  private void setConnectionStatus(ConnectionStatus status, String key) {
    connectionStatus = status;
    statusMessageKey = key;

    switch (status) {
      case Error:
        status_label.setForeground(new Color(255, 0, 0));
        break;
      case Loading:
      case TableNotExists:
        status_label.setForeground(new Color(139, 128, 0));
        break;
      case TableExists:
        status_label.setForeground(new Color(0, 100, 0));
        break;
      default:
        status_label.setForeground(new Color(0, 0, 0));
        break;
    }

    String message = BigQueryStreamMeta.getMessage(key);
    status_label.setText(message);
    status_label.setToolTipText(message);
  }

  /**
   * Test BigQuery credentials
   */
  private boolean checkCredentials() {
    try {
      bigQuery = BigQueryStream.getConnection(useContainerAuth_checkbox.getSelection(), projectId_text.getText(),
          credentialsPath_text.getText());
      return true;
    } catch (Exception e) {
      setConnectionStatus(ConnectionStatus.Error, "BigQueryStream.Messages.ConnectionError");
    }
    return false;
  }

  /**
   * Set checkbox logic on change "History column" value
   */
  private void handleHistoryCol() {
    historyCol_text.setEnabled(historyCol_checkbox.getEnabled() && historyCol_checkbox.getSelection());
    if (!historyCol_checkbox.getEnabled()) {
      historyCol_checkbox.setSelection(!historyCol_text.getText().isEmpty());
    }
    if (!historyCol_checkbox.getSelection()) {
      // historyCol_text.setText("");
    }
  }

  /**
   * Set checkboxes logic on change "Check if exists" value
   */
  private void handleCheckIfExists() {
    skipIfExists_checkbox.setEnabled(checkIfExists_checkbox.getEnabled() && checkIfExists_checkbox.getSelection());
    updateIfExists_checkbox.setEnabled(checkIfExists_checkbox.getEnabled() && checkIfExists_checkbox.getSelection());
    skipIfExists_checkbox.setSelection(checkIfExists_checkbox.getSelection());
    if (!checkIfExists_checkbox.getSelection()) {
      updateIfExists_checkbox.setSelection(checkIfExists_checkbox.getSelection());
    }
  }

  /**
   * Enable/disable table definition grid
   */
  private void setGridEnabled(boolean enabled) {
    setGridEnabled(enabled, null);
  }

  /**
   * Enable/disable table definition grid and set editable or not
   */
  private void setGridEnabled(boolean enabled, Boolean editable) {
    grid.setEnabled(enabled);

    Color bgColor;
    Color fgColor;

    if (enabled) {
      bgColor = display.getSystemColor(SWT.COLOR_LIST_BACKGROUND);
      fgColor = display.getSystemColor(SWT.COLOR_LIST_FOREGROUND);
    } else {
      bgColor = display.getSystemColor(SWT.COLOR_TEXT_DISABLED_BACKGROUND);
      fgColor = display.getSystemColor(SWT.COLOR_WIDGET_DISABLED_FOREGROUND);
    }

    grid.setBackground(bgColor);
    grid.setForeground(fgColor);
    grid.setHeaderBackground(bgColor);
    grid.setHeaderForeground(fgColor);

    if (grid_controls != null) {
      for (int i = 0; i < grid_controls.length; i++) {
        Control control = grid_controls[i];
        if (control instanceof Text || control instanceof Button) {
          control.setEnabled(enabled && (editable == null ? true : editable));
        } else {
          control.setEnabled(enabled);
        }
        control.setBackground(bgColor);
        control.setForeground(fgColor);
      }
    }

    historyCol_checkbox.setEnabled(enabled && (editable == null ? true : editable));
    handleHistoryCol();

    checkIfExists_checkbox.setEnabled(enabled && (editable == null ? true : !editable));
    handleCheckIfExists();
  }

  /**
   * Handles clicking cancel
   */
  private void cancel() {
    if (!loading) {
      changed = input.hasChanged();
      parent.setEnabled(true);
      stepname = null;
      dispose();
    }
  }

  /**
   * Saves data to the meta class instance
   */
  private void ok() {
    if (Const.nullToEmpty(stepName_text.getText()).trim().isEmpty()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BigQueryStreamMeta.getMessage("System.StepJobEntryNameMissing.Title"));
      mb.setMessage(BigQueryStreamMeta.getMessage("System.JobEntryNameMissing.Msg"));
      mb.open();
      return;
    }
    stepname = stepName_text.getText();
    input.setUseContainerSecurity(useContainerAuth_checkbox.getSelection());
    input.setCredentialsPath(credentialsPath_text.getText());
    input.setProjectId(projectId_text.getText());
    input.setDatasetName(datasetName_text.getText());
    input.setTableName(tableName_text.getText());
    input.setSkipIfExists(skipIfExists_checkbox.getSelection());
    input.setUpdateIfExists(updateIfExists_checkbox.getSelection());
    input.setTableInfo(getGridValues());
    BigQueryStreamData.TableInfo.setRefreshPending(stepname, false);

    if (!loading) {
      changed = input.hasChanged();
      parent.setEnabled(true);
      dispose();
    }
  }
}