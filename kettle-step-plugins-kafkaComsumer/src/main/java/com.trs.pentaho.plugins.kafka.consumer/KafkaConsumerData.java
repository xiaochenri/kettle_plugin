package com.trs.pentaho.plugins.kafka.consumer;


import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class KafkaConsumerData extends BaseStepData implements StepDataInterface {

	KafkaConsumer<String,String> consumer;           //消费者
	RowMetaInterface outputRowMeta;
	RowMetaInterface inputRowMeta;
}
