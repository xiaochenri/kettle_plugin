package com.trs.pentaho.step.plugins.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.LabelComboVar;
import org.pentaho.di.ui.core.widget.LabelTextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class TrsRabbitmqDialog extends BaseStepDialog
		implements StepDialogInterface {

	private static Class<?> PKG = TrsRabbitmqDialog.class; // for i18n purposes

	private TrsRabbitmqMeta meta;
	private CTabFolder wTabFolder;
	private FormData fdTabFolder;
	private LabelTextVar usernameLabel, passwordLabel, hostLabel, portLabel,
			exchangeLabel, routingKeyLabel, queueLabel;
	private CTabItem wGeneralTab;
	private Composite wGeneralComp;
	private FormData fdGeneralComp, fdIndexGroup;
	private Group wIndexGroup;

	public TrsRabbitmqDialog(Shell parent, Object baseStepMeta,
							 TransMeta transMeta, String stepname) {
		super(parent, (BaseStepMeta) baseStepMeta, transMeta, stepname);
		meta = (TrsRabbitmqMeta) baseStepMeta;
	}

	@Override
	public String open() {
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent,
				SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook(shell);
		setShellImage(shell, meta);

		changed = meta.hasChanged();

		// 监听器 监听meta对象是否发生改变
		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				meta.setChanged();
			}
		};

		FormLayout layout = new FormLayout();
		layout.marginWidth = Const.FORM_MARGIN;
		layout.marginHeight = Const.FORM_MARGIN;
		shell.setLayout(layout);
		shell.setText(BaseMessages.getString(PKG, "Trs.Rabbitmq.title"));

		//
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		/* 定义一个标签 */
		wlStepname = new Label(shell, SWT.RIGHT);
		/* 设置左边的标签名称 */
		wlStepname
				.setText(BaseMessages.getString(PKG, "System.Label.StepName"));
		props.setLook(wlStepname);

		/* 设置wlStepname的子组件 */
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);

		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		wTabFolder = new CTabFolder(shell, SWT.BORDER);
		props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

		//connect Tab
		addConnectTab(lsMod);
		//model Tab
		addModelTab(lsMod);

		// 确定/取消按钮
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
		setButtonPositions(new Button[] { wOK, wCancel }, margin, null);

		fdTabFolder = new FormData();
		fdTabFolder.left = new FormAttachment(0, 0);
		fdTabFolder.top = new FormAttachment(wStepname, margin);
		fdTabFolder.right = new FormAttachment(100, 0);
		fdTabFolder.bottom = new FormAttachment(wOK, -margin);
		wTabFolder.setLayoutData(fdTabFolder);

		// 确定/取消按钮 监听
		lsCancel = new Listener() {
			public void handleEvent(Event e) {
				cancel();
			}
		};
		lsOK = new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
		};

		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener(SWT.Selection, lsOK);

		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};
		wStepname.addSelectionListener(lsDef);

		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {
				cancel();
			}
		});

		wTabFolder.setSelection(0);

		setSize();

		populateDialog();

		meta.setChanged(changed);

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}

		return stepname;
	}

	private void ok() {

		meta.setUsername(usernameLabel.getText());
		meta.setPassword(passwordLabel.getText());
		meta.setHost(hostLabel.getText());
		meta.setPort(Integer.parseInt(portLabel.getText()));
//		String routingKey = routingKeyLabel.getText();
//		meta.setRoutingKey(routingKey);
//		String exchange = exchangeLabel.getText();
//		meta.setExchange(exchange);
		String queue = queueLabel.getText();
		meta.setQueue(queue);
		String model = modelCombo.getText();
		meta.setModel(model);

		meta.setDurable(durableCombo.getText());
		meta.setExclusive(exclusiveCombo.getText());
		meta.setAutoDelete(autoDeleteCombo.getText());

		meta.setReturnField(returnField.getText());

		if (StringUtils.isNoneBlank(batchSize.getText())){
            meta.setBatchSize(Integer.parseInt(batchSize.getText()));
        }else {
			meta.setBatchSize(0);
		}

		switch (model) {
			case "简单模式":
				if (StringUtils.isBlank(queue)){
                    showMessage("简单模式下需要填写队列名称");
					return;
				}
				break;
			case "工作模式":
				if (StringUtils.isBlank(queue)){
                    showMessage("工作模式下需要填写队列名称");
					return;
				}
				break;
			case "发布、订阅模式":
				if (StringUtils.isBlank(queue)){
                    showMessage("发布、订阅模式下需要填写队列名称");
					return;
				}
//				if (StringUtils.isBlank(exchange)){
//                    showMessage("发布、订阅模式下需要填写exchange名称");
//					return;
//				}
				break;
			case "路由模式":
				if (StringUtils.isBlank(queue)){
                    showMessage("路由模式下需要填写队列名称");
					return;
				}
//				if (StringUtils.isBlank(exchange)){
//                    showMessage("路由模式下需要填写exchange名称");
//					return;
//				}
//				if (StringUtils.isBlank(routingKey)){
//                    showMessage("路由模式下需要填写routingKey名称");
//					return;
//				}
				break;
			case "主题模式":
				if (StringUtils.isBlank(queue)){
                    showMessage("主题模式下需要填写队列名称");
					return;
				}
//				if (StringUtils.isBlank(exchange)){
//                    showMessage("主题模式下需要填写exchange名称");
//					return;
//				}
//				if (StringUtils.isBlank(routingKey)){
//                    showMessage("主题模式下需要填写routingKey名称");
//					return;
//				}
				break;
			default:
				logBasic("未匹配到模式");
				break;
		}

		dispose();
	}

	/**
	 * 取消
	 */
	private void cancel() {
		stepname = null;
		meta.setChanged(changed);
		dispose();
	}

	/**
	 * This helper method puts the step configuration stored in the meta object
	 * and puts it into the dialog controls.
	 */
	private void populateDialog() {
		wStepname.selectAll();

		usernameLabel.setText(Const.NVL(meta.getUsername(), ""));
		passwordLabel.setText(Const.NVL(meta.getPassword(), ""));
		hostLabel.setText(Const.NVL(meta.getHost(), ""));
		portLabel.setText(Const.NVL(meta.getPort()+"", ""));

//		exchangeLabel.setText(Const.NVL(meta.getExchange(), ""));
//		routingKeyLabel.setText(Const.NVL(meta.getRoutingKey(), ""));
		queueLabel.setText(Const.NVL(meta.getQueue(), ""));

		modelCombo.setText(Const.NVL(meta.getModel(),""));

		durableCombo.setText(Const.NVL(meta.getDurable(),""));
		exclusiveCombo.setText(Const.NVL(meta.getExclusive(),""));
		autoDeleteCombo.setText(Const.NVL(meta.getAutoDelete(),""));

		returnField.setText(Const.NVL(meta.getReturnField(),""));

		if (meta.getBatchSize() != null){
            batchSize.setText(meta.getBatchSize().toString());
        }
	}

	private void addConnectTab(ModifyListener lsMod) {

		wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
		wGeneralTab.setText("链接配置");

		wGeneralComp = new Composite(wTabFolder, SWT.NONE);
		props.setLook(wGeneralComp);

		FormLayout generalLayout = new FormLayout();
		generalLayout.marginWidth = 3;
		generalLayout.marginHeight = 3;
		wGeneralComp.setLayout(generalLayout);

		// 链接信息配置
		fillIndexGroup(wGeneralComp, lsMod);

		fdGeneralComp = new FormData();
		fdGeneralComp.left = new FormAttachment(0, 0);
		fdGeneralComp.top = new FormAttachment(wStepname, Const.MARGIN);
		fdGeneralComp.right = new FormAttachment(100, 0);
		fdGeneralComp.bottom = new FormAttachment(100, 0);
		wGeneralComp.setLayoutData(fdGeneralComp);

		wGeneralComp.layout();
		wGeneralTab.setControl(wGeneralComp);
	}

	private void fillIndexGroup(Composite parentTab, ModifyListener lsMod) {
		wIndexGroup = new Group(parentTab, SWT.SHADOW_NONE);
		props.setLook(wIndexGroup);
		wIndexGroup.setText(
				BaseMessages.getString(PKG, "Trs.Connect.Configuration"));

		FormLayout indexGroupLayout = new FormLayout();
		indexGroupLayout.marginWidth = 10;
		indexGroupLayout.marginHeight = 10;
		wIndexGroup.setLayout(indexGroupLayout);

		// host
		hostLabel = new LabelTextVar(transMeta, wIndexGroup,
				BaseMessages.getString(PKG, "Trs.host"),
				BaseMessages.getString(PKG, "Trs.host"));
		hostLabel.addModifyListener(lsMod);

		// port
		portLabel = new LabelTextVar(transMeta, wIndexGroup,
				BaseMessages.getString(PKG, "Trs.port"),
				BaseMessages.getString(PKG, "Trs.port"));
		portLabel.addModifyListener(lsMod);

		// username
		usernameLabel = new LabelTextVar(transMeta, wIndexGroup,
				BaseMessages.getString(PKG, "Trs.username"),
				BaseMessages.getString(PKG, "Trs.username"));
		usernameLabel.addModifyListener(lsMod);

		// password
		passwordLabel = new LabelTextVar(transMeta, wIndexGroup,
				BaseMessages.getString(PKG, "Trs.password"),
				BaseMessages.getString(PKG, "Trs.password"));
		passwordLabel.addModifyListener(lsMod);

		// Test button
		Button wTest = new Button(wIndexGroup, SWT.PUSH);
		wTest.setText("测试连接");

		wTest.addListener(SWT.Selection, new Listener() {
			public void handleEvent(Event arg0) {
				testConnect();
			}
		});

		Control[] connectionControls = new Control[] { hostLabel, portLabel,
				usernameLabel, passwordLabel };
		placeControls(wIndexGroup, connectionControls);

		BaseStepDialog.positionBottomButtons(wIndexGroup,
				new Button[] { wTest }, Const.MARGIN, passwordLabel);

		fdIndexGroup = new FormData();
		fdIndexGroup.left = new FormAttachment(0, Const.MARGIN);
		fdIndexGroup.top = new FormAttachment(wStepname, Const.MARGIN);
		fdIndexGroup.right = new FormAttachment(100, -Const.MARGIN);
		wIndexGroup.setLayoutData(fdIndexGroup);
	}

	private CTabItem modelTab;
	private Composite modelComp;
	private FormData fdModelComp;

	private void addModelTab(ModifyListener lsMod){
		modelTab = new CTabItem(wTabFolder, SWT.NONE);
		modelTab.setText("模式配置");

		modelComp = new Composite(wTabFolder, SWT.NONE);
		props.setLook(modelComp);

		FormLayout generalLayout = new FormLayout();
		generalLayout.marginWidth = 3;
		generalLayout.marginHeight = 3;
		modelComp.setLayout(generalLayout);

		// 链接信息配置
		fillModelGroup(modelComp, lsMod);

		fdModelComp = new FormData();
		fdModelComp.left = new FormAttachment(0, 0);
		fdModelComp.top = new FormAttachment(wStepname, Const.MARGIN);
		fdModelComp.right = new FormAttachment(100, 0);
		fdModelComp.bottom = new FormAttachment(100, 0);
		modelComp.setLayoutData(fdModelComp);

		modelComp.layout();
        modelTab.setControl(modelComp);
	}


	private Group modelGroup;
	private FormData fdModelGroup;
	private LabelComboVar modelCombo;
	private LabelTextVar returnField,batchSize;

	private LabelComboVar durableCombo,exclusiveCombo,autoDeleteCombo;

	private void fillModelGroup(Composite parentTab, ModifyListener lsMod){
		modelGroup = new Group(parentTab, SWT.SHADOW_NONE);
		props.setLook(modelGroup);
		modelGroup.setText(
				BaseMessages.getString(PKG, "Trs.Model.Configuration"));

		FormLayout indexGroupLayout = new FormLayout();
		indexGroupLayout.marginWidth = 10;
		indexGroupLayout.marginHeight = 10;
		modelGroup.setLayout(indexGroupLayout);

		String[] params = {"简单模式"};
		modelCombo = new LabelComboVar(transMeta, modelGroup,
				BaseMessages.getString(PKG, "Trs.Model.name"),
				BaseMessages.getString(PKG, "Trs.Model.name"));
		modelCombo.addModifyListener(lsMod);
		modelCombo.setItems(params);

//		// exchange
//		exchangeLabel = new LabelTextVar(transMeta, modelGroup,
//				BaseMessages.getString(PKG, "Trs.exchange"),
//				BaseMessages.getString(PKG, "Trs.exchange"));
//		exchangeLabel.addModifyListener(lsMod);
//
//		// routingKey
//		routingKeyLabel = new LabelTextVar(transMeta, modelGroup,
//				BaseMessages.getString(PKG, "Trs.routingKey"),
//				BaseMessages.getString(PKG, "Trs.routingKey"));
//		routingKeyLabel.addModifyListener(lsMod);

		// queue
		queueLabel = new LabelTextVar(transMeta, modelGroup,
				BaseMessages.getString(PKG, "Trs.queue"),
				BaseMessages.getString(PKG, "Trs.queue"));
		queueLabel.addModifyListener(lsMod);

		String[] or = { "是", "否" };
		durableCombo = new LabelComboVar(transMeta, modelGroup,
				BaseMessages.getString(PKG, "Trs.durable"),
				BaseMessages.getString(PKG, "Trs.durable"));
		durableCombo.addModifyListener(lsMod);
		durableCombo.setItems(or);

		exclusiveCombo = new LabelComboVar(transMeta, modelGroup,
				BaseMessages.getString(PKG, "Trs.exclusive"),
				BaseMessages.getString(PKG, "Trs.exclusive"));
		exclusiveCombo.addModifyListener(lsMod);
		exclusiveCombo.setItems(or);

		autoDeleteCombo = new LabelComboVar(transMeta, modelGroup,
				BaseMessages.getString(PKG, "Trs.autoDelete"),
				BaseMessages.getString(PKG, "Trs.autoDelete"));
		autoDeleteCombo.addModifyListener(lsMod);
		autoDeleteCombo.setItems(or);

		// returnField
		returnField = new LabelTextVar(transMeta, modelGroup,
				BaseMessages.getString(PKG, "Trs.returnField"),
				BaseMessages.getString(PKG, "Trs.returnField"));
		returnField.addModifyListener(lsMod);


		// returnField
		batchSize = new LabelTextVar(transMeta, modelGroup,
				BaseMessages.getString(PKG, "Trs.batchSize"),
				BaseMessages.getString(PKG, "Trs.batchSize"));
		batchSize.addModifyListener(lsMod);

		Control[] connectionControls = new Control[] { modelCombo, queueLabel , durableCombo , exclusiveCombo , autoDeleteCombo,returnField,batchSize };
		placeControls(modelGroup, connectionControls);

		fdModelGroup = new FormData();
		fdModelGroup.left = new FormAttachment(0, Const.MARGIN);
		fdModelGroup.top = new FormAttachment(wStepname, Const.MARGIN);
		fdModelGroup.right = new FormAttachment(100, -Const.MARGIN);
		modelGroup.setLayoutData(fdModelGroup);
	}

	private void placeControls(Group group, Control[] controls) {

		Control previousAbove = group;
		Control previousLeft = group;

		for (Control control : controls) {
			if (control instanceof Label) {
				addLabelAfter(control, previousAbove);
				previousLeft = control;
			} else {
				addWidgetAfter(control, previousAbove, previousLeft);
				previousAbove = control;
				previousLeft = group;
			}
		}
	}

	private void addWidgetAfter(Control widget, Control widgetAbove,
			Control widgetLeft) {
		props.setLook(widget);
		FormData fData = new FormData();
		fData.left = new FormAttachment(widgetLeft, Const.MARGIN);
		fData.top = new FormAttachment(widgetAbove, Const.MARGIN);
		fData.right = new FormAttachment(100, -Const.MARGIN);
		widget.setLayoutData(fData);
	}

	private void addLabelAfter(Control widget, Control widgetAbove) {
		props.setLook(widget);
		FormData fData = new FormData();
		fData.top = new FormAttachment(widgetAbove, Const.MARGIN);
		fData.right = new FormAttachment(Const.MIDDLE_PCT, -Const.MARGIN);
		widget.setLayoutData(fData);
	}

	private void testConnect() {

		String usernameLabelText = usernameLabel.getText();
		if (StringUtils.isBlank(usernameLabelText)) {
			showError("用户名为空");
			return;
		}
		String passwordLabelText = passwordLabel.getText();
		if (StringUtils.isBlank(passwordLabelText)) {
			showError("密码为空");
			return;
		}
		String hostLabelText = hostLabel.getText();
		if (StringUtils.isBlank(hostLabelText)) {
			showError("链接为空");
			return;
		}
		String portLabelText = portLabel.getText();
		if (StringUtils.isBlank(portLabelText)) {
			showError("端口为空");
			return;
		}

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(hostLabelText);
		factory.setPort(Integer.parseInt(portLabelText));
		factory.setUsername(usernameLabelText);
		factory.setPassword(passwordLabelText);
		// 创建一个连接
		try {
			Connection connection = factory.newConnection();

			if (connection != null) {
				showMessage("连接成功！");
				connection.close();
			} else {
				showError("连接失败！");
			}

		} catch (IOException | TimeoutException e) {
			showError("连接失败！");
			logError(e.getMessage());
		}
	}

	private void showMessage(String message) {
		MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
		mb.setMessage(message);
		mb.setText("tips");
		mb.open();
	}

	private void showError(String message) {
		MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
		mb.setMessage(message);
		mb.setText("tips");
		mb.open();
	}
}
