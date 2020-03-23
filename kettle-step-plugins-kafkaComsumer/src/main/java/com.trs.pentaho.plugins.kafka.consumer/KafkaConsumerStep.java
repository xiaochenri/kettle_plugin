package com.trs.pentaho.plugins.kafka.consumer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

public class KafkaConsumerStep extends BaseStep implements StepInterface {

	private KafkaConsumerMeta trsMeta;
	private KafkaConsumerData trsData;

	private static Boolean exitFlag = false;
	private static Boolean hasExitFlag = false;

	public KafkaConsumerStep(StepMeta stepMeta,
			StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		super.init(smi, sdi);

		trsMeta = (KafkaConsumerMeta) smi;
		trsData = (KafkaConsumerData) sdi;

		Map<String,Object> properties = trsMeta.getKafkaProperties();

		Object o = properties.get("enable.auto.commit");
		if (o != null){
			properties.put("enable.auto.commit",Boolean.parseBoolean(o.toString()));
		}

		properties.put("max.poll.records", trsMeta.getLimit());

		logBasic(properties.toString());

		Thread.currentThread().setContextClassLoader(KafkaConsumer.class.getClassLoader());

		trsData.consumer = new KafkaConsumer<String, String>(properties);

		trsData.consumer.subscribe(Collections.singletonList(trsMeta.getTopic()));

		return true;
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {

		synchronized (exitFlag){
			exitFlag = true;
		}
		super.dispose(smi, sdi);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
			throws KettleException {
		Object[] r = getRow();

		KafkaConsumerMeta meta = (KafkaConsumerMeta) smi;
		final KafkaConsumerData data = (KafkaConsumerData) sdi;

		if (first) {
			first = false;
			data.inputRowMeta = getInputRowMeta();

			if (data.inputRowMeta == null) {
				data.inputRowMeta = new RowMeta();
			}

			meta.getFields(data.inputRowMeta, getStepname(), null, null, null,
					null, null);

			data.outputRowMeta = data.inputRowMeta.clone();
		}

		String name = Thread.currentThread().getName();
		logBasic("处理数据的线程"+name);

		// 持续监听 开始消费
		while (true){
			// poll频率
			ConsumerRecords<String, String> consumerRecords = trsData.consumer
					.poll(1000);

			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

				String message = consumerRecord.value();

				logBasic("===="+message);

				Object[] newRow = getOutputRowData(r,message);

				putRow(data.outputRowMeta, newRow);
			}

			synchronized (exitFlag){
				if (exitFlag){
					trsData.consumer.close();
					break;
				}
			}
		}

		//推出循环的时候就是步骤结束的时候
		setOutputDone();
		return false;
	}

	public void stopRunning(StepMetaInterface smi, StepDataInterface sdi)
			throws KettleException {

		String name = Thread.currentThread().getName();
		logBasic("stopRunning的线程为："+name);

		synchronized (exitFlag){
			exitFlag = true;
		}
		super.stopRunning(smi, sdi);
	}

	private synchronized Object[] getOutputRowData(Object[] row,
												   String messageBody) throws KettleException {

		Object[] outputRowData = new Object[trsData.outputRowMeta.size()];
		;

		// 存在输入
		if (row != null && row.length > 0) {
			for (int i = 0; i < trsData.outputRowMeta.size() - 1; i++) {
				outputRowData[i] = row[i];
			}
		}
		// 在末尾加入本次的数据
		outputRowData[trsData.outputRowMeta.size() - 1] = messageBody;

		return outputRowData;
	}
}
