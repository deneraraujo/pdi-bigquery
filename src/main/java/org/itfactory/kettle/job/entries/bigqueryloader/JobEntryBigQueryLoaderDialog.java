package org.itfactory.kettle.job.entries.bigqueryloader;

import org.pentaho.di.core.util.Utils;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.LabelTextVar;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;


public class JobEntryBigQueryLoaderDialog extends JobEntryDialog implements JobEntryDialogInterface {
    private static Class<?> PKG = JobEntryBigQueryLoader.class;

    private JobEntryBigQueryLoader jobEntry;
    
    private Label wlName;
    private Text wName;
  
    private FormData fdlName, fdName;

    private Label dellName;
    private Text delName;
    private FormData fdelName;
    
    private Label cplName;
    private Text cpName;
    private FormData fcplName,fcpName;
    
    private Label plName;
    private Text pName;
    private FormData fplName,fpName;

    private Label dslName;
    private Text dsName;
    private FormData fdslName,fdsName;
    
    private Label tlName;
    private Text tName;
    private FormData ftlName,ftName;
    
    private Label slName;
    private Text sName;
    private FormData fslName,fsName;
    
    private Label qlName;
    private Text qName;
    private FormData fqlName,fqName;

    private Button wOK, wCancel;
  
    private Listener lsOK, lsCancel;
    
    private Shell shell;
  
    private SelectionAdapter lsDef;

    private CTabFolder wTabFolder;
    private Composite wGeneralComp, wFilesComp;
    private CTabItem wGeneralTab;
  
    private boolean changed = false;

    public JobEntryBigQueryLoaderDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        this.jobEntry = (JobEntryBigQueryLoader) jobEntryInt;
        if ( this.jobEntry.getName() == null ) {
          this.jobEntry.setName( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Name.Default" ) );
        }
    }

    public JobEntryInterface open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();
    
        shell = new Shell( parent, props.getJobsDialogStyle() );
        props.setLook( shell );
        JobDialog.setShellImage( shell, jobEntry );
    
        ModifyListener lsMod = new ModifyListener() {
          public void modifyText( ModifyEvent e ) {
            //sftpclient = null;
            jobEntry.setChanged();
          }
        };
        changed = jobEntry.hasChanged();
    
        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;
    
        shell.setLayout( formLayout );
        shell.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Title" ) );
    
        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;
    
        // Step Name
        wlName = new Label( shell, SWT.RIGHT );
        wlName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Name.Label" ) );
        props.setLook( wlName );
        fdlName = new FormData();
        fdlName.left = new FormAttachment( 0, 0 );
        fdlName.right = new FormAttachment( middle, -margin );
        fdlName.top = new FormAttachment( 0, margin );
        wlName.setLayoutData( fdlName );
        wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wName );
        wName.addModifyListener( lsMod );
        fdName = new FormData();
        fdName.left = new FormAttachment( middle, 0 );
        fdName.top = new FormAttachment( 0, margin );
        fdName.right = new FormAttachment( 100, 0 );
        wName.setLayoutData( fdName );

        // Credentials path
        cplName = new Label( shell, SWT.RIGHT );
        cplName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.CredentialsPath.Label" ) );
        props.setLook( cplName );
        fcplName = new FormData();
        fcplName.left = new FormAttachment( 0, 0 );
        fcplName.right = new FormAttachment( middle, -margin );
        fcplName.top = new FormAttachment( wlName, margin );
        cplName.setLayoutData( fcplName );
        cpName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( cpName );
        cpName.addModifyListener( lsMod );
        fcpName = new FormData();
        fcpName.left = new FormAttachment( middle, 0 );
        fcpName.top = new FormAttachment( wlName, margin );
        fcpName.right = new FormAttachment( 100, 0 );
        cpName.setLayoutData( fcpName );

        // BigQuery Project Id
        plName = new Label( shell, SWT.RIGHT );
        plName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Project.Label" ) );
        props.setLook( plName );
        fplName = new FormData();
        fplName.left = new FormAttachment( 0, 0 );
        fplName.right = new FormAttachment( middle, -margin );
        fplName.top = new FormAttachment( cpName, margin );
        plName.setLayoutData( fplName );
        pName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( pName );
        pName.addModifyListener( lsMod );
        fpName = new FormData();
        fpName.left = new FormAttachment( middle, 0 );
        fpName.top = new FormAttachment( cpName, margin );
        fpName.right = new FormAttachment( 100, 0 );
        pName.setLayoutData( fpName );

        // BigQuery dataset name
        dslName = new Label( shell, SWT.RIGHT );
        dslName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.DataSet.Label" ) );
        props.setLook( dslName );
        fdslName = new FormData();
        fdslName.left = new FormAttachment( 0, 0 );
        fdslName.right = new FormAttachment( middle, -margin );
        fdslName.top = new FormAttachment( pName, margin );
        dslName.setLayoutData( fdslName );
        dsName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( dsName );
        dsName.addModifyListener( lsMod );
        fdsName = new FormData();
        fdsName.left = new FormAttachment( middle, 0 );
        fdsName.top = new FormAttachment( pName, margin );
        fdsName.right = new FormAttachment( 100, 0 );
        dsName.setLayoutData( fdsName );

        // table name
        tlName = new Label( shell, SWT.RIGHT );
        tlName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Table.Label" ) );
        props.setLook( tlName );
        ftlName = new FormData();
        ftlName.left = new FormAttachment( 0, 0 );
        ftlName.right = new FormAttachment( middle, -margin );
        ftlName.top = new FormAttachment( dsName, margin );
        tlName.setLayoutData( ftlName );
        tName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( tName );
        tName.addModifyListener( lsMod );
        ftName = new FormData();
        ftName.left = new FormAttachment( middle, 0 );
        ftName.top = new FormAttachment( dslName, margin );
        ftName.right = new FormAttachment( 100, 0 );
        tName.setLayoutData( ftName );

        // GCS source URI
        slName = new Label( shell, SWT.RIGHT );
        slName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Source.Label" ) );
        props.setLook( slName );
        fslName = new FormData();
        fslName.left = new FormAttachment( 0, 0 );
        fslName.right = new FormAttachment( middle, -margin );
        fslName.top = new FormAttachment( tName, margin );
        slName.setLayoutData( fslName );
        sName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( sName );
        sName.addModifyListener( lsMod );
        fsName = new FormData();
        fsName.left = new FormAttachment( middle, 0 );
        fsName.top = new FormAttachment( tName, margin );
        fsName.right = new FormAttachment( 100, 0 );
        sName.setLayoutData( fsName );

        // CSV delimiter
        dellName = new Label( shell, SWT.RIGHT );
        dellName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Delimiter.Label" ) );
        props.setLook( dellName );
        fdelName = new FormData();
        fdelName.left = new FormAttachment( 0, 0 );
        fdelName.right = new FormAttachment( middle, -margin );
        fdelName.top = new FormAttachment( sName, margin );
        dellName.setLayoutData( fdelName );
        delName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( delName );
        delName.addModifyListener( lsMod );
        fdelName = new FormData();
        fdelName.left = new FormAttachment( middle, 0 );
        fdelName.top = new FormAttachment( sName, margin );
        fdelName.right = new FormAttachment( 100, 0 );
        delName.setLayoutData( fdelName );

        // quote
        qlName = new Label( shell, SWT.RIGHT );
        qlName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Quote.Label" ) );
        props.setLook( qlName );
        fqlName = new FormData();
        fqlName.left = new FormAttachment( 0, 0 );
        fqlName.right = new FormAttachment( middle, -margin );
        fqlName.top = new FormAttachment( delName, margin );
        qlName.setLayoutData( fqlName );
        qName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( qName );
        qName.addModifyListener( lsMod );
        fqName = new FormData();
        fqName.left = new FormAttachment( middle, 0 );
        fqName.top = new FormAttachment( delName, margin );
        fqName.right = new FormAttachment( 100, 0 );
        qName.setLayoutData( fqName );

        /*
    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

        wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
        wGeneralTab.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Tab.General.Label" ) );
    
        wGeneralComp = new Composite( wTabFolder, SWT.NONE );
        props.setLook( wGeneralComp );
    
        FormLayout generalLayout = new FormLayout();
        generalLayout.marginWidth = 3;
        generalLayout.marginHeight = 3;
        wGeneralComp.setLayout( generalLayout );
*/
        
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, qName );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );


    getData();
    //activeCopyFromPrevious();
    //activeUseKey();

    BaseStepDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "GoogleBigQueryStorageLoadDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
    }
    
    public void getData() {
        wName.setText( Const.nullToEmpty( jobEntry.getName() ) );
        cpName.setText( Const.nullToEmpty( jobEntry.getCredentialsPath() ) );
        pName.setText( Const.nullToEmpty( jobEntry.getProjectId() ) );
        delName.setText( Const.nullToEmpty( jobEntry.getDelimiter() ) );
        dsName.setText( Const.nullToEmpty( jobEntry.getDatasetName() ) );
        tName.setText( Const.nullToEmpty( jobEntry.getTableName() ) );
        sName.setText( Const.nullToEmpty( jobEntry.getSourceUri()) ) ;
        qName.setText( Const.nullToEmpty( jobEntry.getQuote()) ) ;
    }
    
  private void ok() {
    if (null == wName.getText() || wName.getText().length() == 0 ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }
    jobEntry.setName( wName.getText() );
    jobEntry.setCredentialsPath(cpName.getText() );
    jobEntry.setProjectId( pName.getText() );
    jobEntry.setDelimiter( delName.getText() );
    jobEntry.setDatasetName( dsName.getText() );
    jobEntry.setTableName( tName.getText() );
    jobEntry.setSourceUri( sName.getText() );
    jobEntry.setQuote( qName.getText() );
    dispose();
  }
  
  private void cancel() {
    jobEntry.setChanged( changed );
    jobEntry = null;
    dispose();
  }

  public void dispose() {
    //closeFTPConnections();
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }
}
