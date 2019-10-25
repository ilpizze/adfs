package it.antonio.adfs.comunication;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;
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
						
						String originalMessageId = message.getStringProperty("requestId"); 
						
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
	public InputStream read(String filename) {
		try {
			
			
			return internalRead(filename, 10); // max tries = 10
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	private InputStream internalRead(String key, int tryCount) throws Exception {
		if(tryCount == 0) {
			return null;
		}
		
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
		
		String requestId = "request_" + UUID.randomUUID().toString();
		message.setStringProperty("requestId", requestId);	
		
		CountDownLatch latch = new CountDownLatch(1);
		cdl.put(requestId, latch);
		
		
		slaveProducer.send(message);
		
		latch.await(120, TimeUnit.SECONDS);
		
		InputStream stream = responses.remove(requestId);
		if(stream == null) {
			throw new IllegalStateException();
		}
		
		if(stream instanceof NullInputStream) {
			return internalRead(key, tryCount - 1);
		} else {
			return stream;
		}
	}
	
	private static class NullInputStream extends InputStream{

		@Override
		public int read() throws IOException {
			return 0;
		}
		
	}

}
