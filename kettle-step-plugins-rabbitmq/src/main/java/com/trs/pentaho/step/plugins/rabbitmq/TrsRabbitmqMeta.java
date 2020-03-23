package com.trs.pentaho.step.plugins.rabbitmq;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.w3c.dom.Node;

@Step(id = "trsRabbitmq", name = "rabbitmq消费", description = "rabbitmq消费", image = "image/demo.png", categoryDescription = "trs"// 插件在kettle中的分类
)
@Getter
@Setter
@ToString
public class TrsRabbitmqMeta extends BaseStepMeta implements StepMetaInterface {

	private static final Class<?> PKG = TrsRabbitmqMeta.class; // for i18n

	private String username;
	private String password;
	private String host;
	private int port;
//	private String exchange; // exchange,routingKey是由生产者提供给消费方的
//	private String routingKey;

	private String queue; // 消费队列名称，将声明的队列通过routing key绑定到exchange，名称可自定义

	private String model; // 模式

	private String durable;
	private String exclusive;
	private String autoDelete;

	private String returnField;

	private Integer batchSize;//批量大小

	public TrsRabbitmqMeta() {
		super();
	}

	@Override
	public void setDefault() {

		this.port = 5672;
		this.model = "简单模式";

		this.durable = "是";
		this.exclusive = "否";
		this.autoDelete = "否";

		this.returnField = "trs_rabbitmq_message";

		this.batchSize = 0;
	}

	@Override
	public StepInterface getStep(StepMeta stepMeta,
			StepDataInterface stepDataInterface, int i, TransMeta transMeta,
			Trans trans) {
		return new TrsRabbitmq(stepMeta, stepDataInterface, i, transMeta,
				trans);
	}

	@Override
	public void getFields(RowMetaInterface inputRowMeta, String name,
			RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
			Repository repository, IMetaStore metaStore)
			throws KettleStepException {

		if (StringUtils.isNoneBlank(returnField)){
			ValueMetaInterface valueMeta = new ValueMetaString(returnField);

			valueMeta.setOrigin(name);
			// add if doesn't exist
			if (!inputRowMeta.exists(valueMeta)) {
				inputRowMeta.addValueMeta(valueMeta);
			}

		}
	}

	@Override
	public StepDataInterface getStepData() {
		return new TrsRabbitmqData();
	}

	@Override
	public void saveRep(Repository rep, IMetaStore metaStore,
			ObjectId id_transformation, ObjectId id_step)
			throws KettleException {
		rep.saveStepAttribute(id_transformation, id_step, "username", username);
		rep.saveStepAttribute(id_transformation, id_step, "password", password);
		rep.saveStepAttribute(id_transformation, id_step, "host", host);
		rep.saveStepAttribute(id_transformation, id_step, "port", port);
//		rep.saveStepAttribute(id_transformation, id_step, "exchange", exchange);
//		rep.saveStepAttribute(id_transformation, id_step, "routingKey",
//				routingKey);
		rep.saveStepAttribute(id_transformation, id_step, "queue", queue);
		rep.saveStepAttribute(id_transformation, id_step, "model", model);

		rep.saveStepAttribute(id_transformation, id_step, "durable", durable);
		rep.saveStepAttribute(id_transformation, id_step, "exclusive",
				exclusive);
		rep.saveStepAttribute(id_transformation, id_step, "autoDelete",
				autoDelete);

		rep.saveStepAttribute(id_transformation, id_step, "returnField",
				returnField);

		rep.saveStepAttribute(id_transformation,id_step,"batchSize",batchSize);
	}

	@Override
	public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step,
			List<DatabaseMeta> databases) throws KettleException {
		username = rep.getStepAttributeString(id_step, "username");
		password = rep.getStepAttributeString(id_step, "password");
		host = rep.getStepAttributeString(id_step, "host");
		port = (int) rep.getStepAttributeInteger(id_step, "port");
//		exchange = rep.getStepAttributeString(id_step, "exchange");
//		routingKey = rep.getStepAttributeString(id_step, "routingKey");
		queue = rep.getStepAttributeString(id_step, "queue");
		model = rep.getStepAttributeString(id_step, "model");

		durable = rep.getStepAttributeString(id_step, "durable");
		exclusive = rep.getStepAttributeString(id_step, "exclusive");
		autoDelete = rep.getStepAttributeString(id_step, "autoDelete");

		returnField = rep.getStepAttributeString(id_step,"returnField");

		batchSize = (int) rep.getStepAttributeInteger(id_step,"batchSize");
	}

	@Override
	public String getXML() throws KettleException {

		StringBuffer retval = new StringBuffer();
		Indentation indent = new Indentation();

		indent.incr().incr();

		// General
		retval.append(indent.toString()).append(XMLHandler.openTag(Dom.TAG_GENERAL)).append(Const.CR);
		indent.incr();

		retval.append(indent.toString() + XMLHandler.addTagValue("username", getUsername()));
		retval.append(indent.toString() + XMLHandler.addTagValue("password", getPassword()));
		retval.append(indent.toString() + XMLHandler.addTagValue("host", getHost()));
		retval.append(indent.toString() + XMLHandler.addTagValue("port", getPort()));
		retval.append(indent.toString() + XMLHandler.addTagValue("queue", getQueue()));
		retval.append(indent.toString() + XMLHandler.addTagValue("model", getModel()));
		retval.append(indent.toString() + XMLHandler.addTagValue("durable", getDurable()));
		retval.append(indent.toString() + XMLHandler.addTagValue("exclusive", getExclusive()));
		retval.append(indent.toString() + XMLHandler.addTagValue("autoDelete", getAutoDelete()));
		retval.append(indent.toString() + XMLHandler.addTagValue("returnField", getReturnField()));
		retval.append(indent.toString() + XMLHandler.addTagValue("batchSize", getBatchSize()));

		indent.decr();
		retval.append(indent.toString()).append(XMLHandler.closeTag(Dom.TAG_GENERAL)).append(Const.CR);

		return retval.toString();
	}

	@Override
	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {

		Node general = XMLHandler.getSubNode(stepnode, Dom.TAG_GENERAL);

		setUsername(XMLHandler.getTagValue(general, "username"));
		setPassword(XMLHandler.getTagValue(general, "password"));
		setHost(XMLHandler.getTagValue(general, "host"));
		setPort(Integer.parseInt(XMLHandler.getTagValue(general, "port")));
		setQueue(XMLHandler.getTagValue(general, "queue"));
		setModel(XMLHandler.getTagValue(general, "model"));
		setDurable(XMLHandler.getTagValue(general, "durable"));
		setExclusive(XMLHandler.getTagValue(general, "exclusive"));
		setAutoDelete(XMLHandler.getTagValue(general, "autoDelete"));
		setReturnField(XMLHandler.getTagValue(general, "returnField"));
		setBatchSize(Integer.valueOf(XMLHandler.getTagValue(general, "batchSize")));
	}

	private static class Indentation {

		private static String indentUnit = Dom.INDENT;
		private String indent = "";
		private int indentLevel = 0;

		public Indentation incr() {
			indentLevel++;
			indent += indentUnit;
			return this;
		}

		public Indentation decr() {
			if (--indentLevel >= 0) {
				indent = indent.substring(0, indent.length() - indentUnit.length());
			}
			return this;
		}

		public String toString() {
			return indent;
		}

	}

	/**
	 * Serialization aids
	 */
	private static class Dom {
		static final String TAG_GENERAL = "general";

		static final String INDENT = "  ";
	}
}
