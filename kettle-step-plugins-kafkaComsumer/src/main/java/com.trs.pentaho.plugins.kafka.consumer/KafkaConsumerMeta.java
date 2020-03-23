package com.trs.pentaho.plugins.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
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

@ToString
@Getter
@Setter
@Step(id = "trsKafkaConsumer", name = "kafka消费", description = "kafka消费", image = "image/demo.png", categoryDescription = "trs"// 插件在kettle中的分类
)
public class KafkaConsumerMeta extends BaseStepMeta implements StepMetaInterface {

	private static final Class<?> clazz = KafkaConsumerMeta.class;

	public static final String[] KAFKA_PROPERTIES_NAMES = new String[] { "bootstrap.servers", "group.id", "key.deserializer",
			"value.deserializer", "auto.offset.reset", "enable.auto.commit" };
	public static final Map<String, String> KAFKA_PROPERTIES_DEFAULTS = new HashMap<String, String>();
	static {
		KAFKA_PROPERTIES_DEFAULTS.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		KAFKA_PROPERTIES_DEFAULTS.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		KAFKA_PROPERTIES_DEFAULTS.put("auto.offset.reset","latest");
		KAFKA_PROPERTIES_DEFAULTS.put("enable.auto.commit","true");
	}

	private Map<String,Object> kafkaProperties = new HashMap<>();
	private String topic;          //topic
	private String returnField;          //field
	private String limit;          //消费数量

	public void setDefault() {

		this.returnField = "message";
		this.limit = "2000";

	}

	@Override
	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository, IMetaStore metaStore) {
		if (topic == null) {
			remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
					BaseMessages.getString(clazz,"KafkaConsumerMeta.Check.InvalidTopic"), stepMeta));
		}
		if (returnField == null) {
			remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
					BaseMessages.getString(clazz,"KafkaConsumerMeta.Check.InvalidField"), stepMeta));
		}
	}

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
			Trans trans) {
		return new KafkaConsumerStep(stepMeta, stepDataInterface, cnr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new KafkaConsumerData();
	}


	@Override
	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
		try {
			topic = XMLHandler.getTagValue(stepnode, "TOPIC");
			returnField = XMLHandler.getTagValue(stepnode, "RETURN_FIELD");
			limit = XMLHandler.getTagValue(stepnode, "LIMIT");
			// This tag only exists if the value is "true", so we can directly
			// populate the field
			Node kafkaNode = XMLHandler.getSubNode(stepnode, "KAFKA");
			for (String name : KAFKA_PROPERTIES_NAMES) {
				String value = XMLHandler.getTagValue(kafkaNode, name);
				if (value != null) {
					kafkaProperties.put(name, value);
				}
			}

            logBasic("loadXml:"+kafkaProperties.toString());

		} catch (Exception e) {
			throw new KettleXMLException(BaseMessages.getString(clazz,"KafkaConsumerMeta.Exception.loadXml"), e);
		}
	}

    /**ctrl+s 时执行
     * @return
     * @throws KettleException
     */
	public String getXML() throws KettleException {
		StringBuilder retval = new StringBuilder();
		if (topic != null) {
			retval.append("    ").append(XMLHandler.addTagValue("TOPIC", topic));
		}
		if (returnField != null) {
			retval.append("    ").append(XMLHandler.addTagValue("RETURN_FIELD", returnField));
		}
		if (limit != null) {
			retval.append("    ").append(XMLHandler.addTagValue("LIMIT", limit));
		}
		retval.append("    ").append(XMLHandler.openTag("KAFKA")).append(Const.CR);
		for (String name : KAFKA_PROPERTIES_NAMES) {
			Object value = kafkaProperties.get(name);
			if (value != null) {
				retval.append("      " + XMLHandler.addTagValue(name, value.toString()));
			}
		}
		retval.append("    ").append(XMLHandler.closeTag("KAFKA")).append(Const.CR);

		logBasic("getXml:"+retval.toString());

		return retval.toString();
	}

	@Override
	public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException {
		try {
			topic = rep.getStepAttributeString(id_step, "TOPIC");
			returnField = rep.getStepAttributeString(id_step, "RETURN_FIELD");
			limit = rep.getStepAttributeString(id_step, "LIMIT");
			for (String name : KAFKA_PROPERTIES_NAMES) {
				String value = rep.getStepAttributeString(id_step, name);
				if (value != null) {
					kafkaProperties.put(name, value);
				}
			}
		} catch (Exception e) {
			throw new KettleException("KafkaConsumerMeta.Exception.loadRep", e);
		}
	}


	@Override
	public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
		try {
			if (topic != null) {
				rep.saveStepAttribute(id_transformation, id_step, "TOPIC", topic);
			}
			if (returnField != null) {
				rep.saveStepAttribute(id_transformation, id_step, "RETURN_FIELD", returnField);
			}
			if (limit != null) {
				rep.saveStepAttribute(id_transformation, id_step, "LIMIT", limit);
			}
			for (String name : KAFKA_PROPERTIES_NAMES) {
				Object value = kafkaProperties.get(name);
				if (value != null) {
					rep.saveStepAttribute(id_transformation, id_step, name, value.toString());
				}
			}
		} catch (Exception e) {
			throw new KettleException("KafkaConsumerMeta.Exception.saveRep", e);
		}
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
}
