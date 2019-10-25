package it.antonio.adfs.comunication;

import java.io.InputStream;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;

import it.antonio.adfs.store.BlobStore;

public class ActiveMQSlaveReader implements SlaveReader{
	
	private String slaveName;
	private ActiveMQSession session;
	private BlobStore store;
	
	private Queue slaveQueue;
	
	private MessageProducer masterProducer;
	private MessageConsumer slaveConsumer;
	
	
	
	public ActiveMQSlaveReader(String slaveName, ActiveMQSession session,BlobStore store) {
		super();
		this.session = session;
		this.store = store;
		this.slaveName = slaveName;
	}

	@Override
	public void init() {
		try {
			
			
			Queue masterQueue = session.createQueue("queue.master.receiver");
			masterProducer = session.createProducer(masterQueue);

			
			slaveQueue = session.createQueue("queue.master." + slaveName);
			slaveConsumer = session.createConsumer(slaveQueue);
			
			slaveConsumer.setMessageListener(new MessageListener() {
				
				@Override
				public void onMessage(Message message) {
					
					try {
						String key = message.getStringProperty("key");
						String requestId = message.getStringProperty("requestId");
						InputStream stream = store.get(key);
						
						if(stream != null) {
							BlobMessage blobMessage = session.createBlobMessage(stream);
							blobMessage.setStringProperty("requestId", requestId);
							blobMessage.setBooleanProperty("empty", false);
							masterProducer.send(blobMessage);
							
						} else {
							Message emptyMessage = session.createMessage();
							emptyMessage.setStringProperty("requestId", requestId);
							emptyMessage.setBooleanProperty("empty", true);
							masterProducer.send(emptyMessage);
							
						}
						
						
						
						
					} catch (JMSException e) {
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


	

	

	
}
