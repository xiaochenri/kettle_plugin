package com.trs.pentaho.step.plugins.rabbitmq;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import com.rabbitmq.client.*;

public class TrsRabbitmq extends BaseStep implements StepInterface {

	private TrsRabbitmqMeta trsMeta;
	private TrsRabbitmqData trsData;

	private Connection connection;
	private Channel channel;

	private String queue;
	private boolean autoAck = false; // 自动ack
	private String encoding = "UTF-8";

	private Integer batchSize;

	private int count = 0;
	private long lastDeliveryTag = 0;
	private long lastReceiveTime = 0;

	private Map<Long, Object[]> map = new LinkedHashMap<>(); // 保证按顺序消费

	public TrsRabbitmq(StepMeta stepMeta, StepDataInterface stepDataInterface,
			int copyNr, TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	@Override
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
			throws KettleException {

		Object[] row = getRow();

		if (first) {
			first = false;

			setupData(); // 输出数据 根据是否存在上一步骤的数据，确定输出数据

			RowMetaInterface outputRowMeta = trsData.outputRowMeta;
			String[] fieldNames = outputRowMeta.getFieldNames();
			for (String fieldName : fieldNames) {
				logBasic("=================" + fieldName);
			}

			try {
				// 声明消费者
				Consumer consumer = new DefaultConsumer(channel) {

					@Override
					public void handleDelivery(String consumerTag,
							Envelope envelope, AMQP.BasicProperties properties,
							byte[] body) throws IOException {

						String messageBody = new String(body, encoding);
						long deliveryTag = envelope.getDeliveryTag();

						try {
							Object[] outputRowData = getOutputRowData(row,
									messageBody);

							// 此处分为两种处理情况，当不输入批处理数量时，直接接收确认数据往下流
							// 当有批处理数量时，此时批量确认数据往下流，多线程需要同步
							if (batchSize == null || batchSize == 0) {

								putRow(trsData.outputRowMeta, outputRowData);

								channel.basicAck(deliveryTag, false);
							} else {
								synchronized (map) {
									map.put(deliveryTag, outputRowData);
								}
							}

						} catch (KettleException e) {
							logError(e.getMessage());
						}
					}

				};

				channel.basicConsume(queue, false, consumer);

			} catch (Exception e) {
				logError(e.getMessage());
			}
		}

		if (batchSize != null && batchSize > 0) {
			synchronized (map) {
				if (map.size() == trsMeta.getBatchSize()) {
					map.forEach((key, value) -> {
						try {
							putRow(trsData.outputRowMeta, value);
						} catch (KettleStepException e) {
							logError("向下推送出错");
							logError(e.getMessage());
						}

						try {
							channel.basicAck(key, false);
						} catch (IOException e) {
							logError("确认消息出错");
							logError(e.getMessage());
						}
					});
					logBasic("一次推送数据量" + map.size());
					map.clear();
				}
			}
		}

		return true;
	}

	@Override
	public boolean init(StepMetaInterface stepMetaInterface,
			StepDataInterface stepDataInterface) {

		trsMeta = (TrsRabbitmqMeta) stepMetaInterface;
		trsData = (TrsRabbitmqData) stepDataInterface;

		if (super.init(trsMeta, trsData)) {

			logBasic("开始初始化");

			try {

				String host = trsMeta.getHost();
				int port = trsMeta.getPort();
				String username = trsMeta.getUsername();
				String password = trsMeta.getPassword();
				String exchange = "";
				String routingKey = "";
				queue = trsMeta.getQueue();
				batchSize = trsMeta.getBatchSize();

				logBasic(batchSize+"");

				boolean durable = trsMeta.getDurable().equals("是");
				boolean exclusive = trsMeta.getExclusive().equals("是");
				boolean autoDelete = trsMeta.getAutoDelete().equals("是");

				logBasic(username + password + exchange + routingKey + queue
						+ host + port);

				if (StringUtils.isNoneBlank(host, username, password)) {
					ConnectionFactory factory = new ConnectionFactory();
					factory.setUsername(username);
					factory.setPassword(password);
					factory.setHost(host);
					factory.setPort(port);

					connection = factory.newConnection();
					channel = connection.createChannel();

					// durable是否持久化，exclusive是否声明一个排他队列，autoDelete是否申明自动删除队列

					String model = trsMeta.getModel();

					logBasic("=======" + model);

					switch (model) {
					case "简单模式":
						// 声明队列
						channel.queueDeclare(queue, durable, exclusive,
								autoDelete, null);
						break;
					case "工作模式":
						// 声明队列
						channel.queueDeclare(queue, durable, exclusive,
								autoDelete, null);
						break;
					case "发布、订阅模式": // 生产者端发送消息，多个消费者同时接收所有的消息。
						// 声明交换机
						channel.exchangeDeclare(exchange, "fanout");
						// 声明队列
						channel.queueDeclare(queue, durable, exclusive,
								autoDelete, null);
						// 绑定队列到交换机
						channel.queueBind(queue, exchange, "");
						break;
					case "路由模式": // 在绑定queue和exchange的时候使用了路由routing
									// key，即从该exchange上只接收routing key指定的消息。
						channel.exchangeDeclare(exchange, "direct");
						channel.queueDeclare(queue, durable, exclusive,
								autoDelete, null);
						channel.queueBind(queue, exchange, routingKey);
						break;
					case "主题模式": // 发送消息的routing
									// key不是固定的单词，而是匹配字符串，如"china.#"，*匹配一个单词，#匹配0个或多个单词。因此如“china.#”能够匹配到“china.news.info”，但是“china.*
									// ”只会匹配到“china.news”
						channel.exchangeDeclare(exchange, "topic");
						channel.queueDeclare(queue, durable, exclusive,
								autoDelete, null);
						channel.queueBind(queue, exchange, routingKey);
						break;
					default:
						logBasic("未匹配到模式");
						break;
					}

					return true;
				}

			} catch (Exception e) {
				logError(e.getMessage());
			}
		}

		logBasic("初始化失败");

		return false;
	}

	@Override
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {

		if (channel != null) {
			try {
				channel.close();
			} catch (IOException | TimeoutException e) {
				logError(e.getMessage());
			}
		}

		if (connection != null) {
			try {
				connection.close();
			} catch (IOException e) {
				logError(e.getMessage());
			}
		}

		super.dispose(smi, sdi);
	}

	private Object[] createOutputRow(Object[] inputRow, int outputRowSize) {

		return RowDataUtil.createResizedCopy(inputRow, outputRowSize);
	}

	private void setupData() throws KettleStepException {

		RowMetaInterface inputRowMeta = getInputRowMeta();

		if (inputRowMeta != null) {
			RowMetaInterface clone = inputRowMeta.clone();
			trsData.inputRowMeta = clone;

			RowMetaInterface clone1 = clone.clone();
			clone1.addValueMeta(new ValueMetaString(trsMeta.getReturnField()));
			trsData.outputRowMeta = clone1;
		} else {
			trsData.outputRowMeta = getRowMeta();
		}

	}

	/**
	 * Creates a rowMeta for output field names
	 */
	private RowMetaInterface getRowMeta() {
		RowMeta rowMeta = new RowMeta();
		rowMeta.addValueMeta(new ValueMetaString(trsMeta.getReturnField()));
		return rowMeta;
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
