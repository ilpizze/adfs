package it.antonio.adfs.comunication;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;

public class ActiveMQMasterReader implements MasterReader{
	

	private ActiveMQSession session;
	private SlaveRepository slaveRepository;
	
	private Queue masterQueue;
	
	private MessageConsumer slaveConsumer;
	private Map<String, InputStream> responses = new ConcurrentHashMap<String, InputStream>();
	private Map<String, CountDownLatch> cdl = new ConcurrentHashMap<>();
	
	private Map<String, MessageProducer> producers = new ConcurrentHashMap<>();
	
	public ActiveMQMasterReader(ActiveMQSession session, SlaveRepository slaveRepository) {
		super();
		this.session = session;
		this.slaveRepository = slaveRepository;
	}

	@Override
	public void init() {
		try {
			
			
			//slaveQueue = session.createQueue("queue.master.receiver.slave1");
			//slaveProducer = session.createProducer(slaveQueue);

			masterQueue = session.createQueue("queue.master.receiver");
			slaveConsumer = session.createConsumer(masterQueue);
			
			slaveConsumer.setMessageListener(new MessageListener() {
				
				@Override
				public void onMessage(Message message) {
					
					try {
						
						String originalMessageId = message.getJMSCorrelationID(); 
						
						if(message.getBooleanProperty("empty") == false) {
							BlobMessage blobMessage = (BlobMessage) message; 
							responses.put(originalMessageId, blobMessage.getInputStream());
						} else {
							responses.put(originalMessageId, new NullInputStream());
						}
						
						
						CountDownLatch latch = cdl.remove(originalMessageId);
						latch.countDown();
					} catch (JMSException | IOException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}
			});
			
		}catch(Throwable t) {
			t.printStackTrace();
			throw new RuntimeException(t);
		}
		
		
	}

	

	

	@Override
	public InputStream read(String key) {
		try {
			
			String slave = slaveRepository.randomSlave();
			
			MessageProducer slaveProducer = producers.get(slave);
			if(slaveProducer == null) {
				synchronized (producers) {
					Queue queue = session.createQueue("queue.master." + slave);
					slaveProducer = session.createProducer(queue);
					producers.put(slave, slaveProducer);
				}
			}
			
			
			Message message = session.createMessage();
			message.setStringProperty("key", key);
			
			slaveProducer.send(message);
			
			String messageId = message.getJMSMessageID();
			
			CountDownLatch latch = new CountDownLatch(1);
			
			cdl.put(messageId, latch);
			latch.await(60, TimeUnit.SECONDS);
			
			InputStream stream = responses.get(messageId);
			
			if(stream instanceof NullInputStream) {
				return null;
			} else {
				return stream;
			}
			
		} catch (JMSException | InterruptedException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	private static class NullInputStream extends InputStream{

		@Override
		public int read() throws IOException {
			return 0;
		}
		
	}

}
