package it.antonio.adfs.comunication;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;

import org.apache.activemq.ActiveMQSession;

public class ActiveMQMasterSync implements MasterSync {

	private ActiveMQSession session;
	
	private Map<String, MessageProducer> producers = new ConcurrentHashMap<>();

	public ActiveMQMasterSync(ActiveMQSession session) {
		super();
		this.session = session;
	}

	@Override
	public void init() {

	}

	@Override
	public void sync(String slave) {
		try {

			MessageProducer slaveProducer = producers.get(slave);
			if (slaveProducer == null) {
				synchronized (producers) {
					Queue queue = session.createQueue("queue.master." + slave + ".sync");
					slaveProducer = session.createProducer(queue);
					producers.put(slave, slaveProducer);
				}
			}

			Message message = session.createMessage();

			slaveProducer.send(message);

		} catch (JMSException e) {
			throw new RuntimeException(e);
		}

	}

}
