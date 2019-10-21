package it.antonio.adfs.comunication;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;

import it.antonio.adfs.store.BlobStore;
import it.antonio.adfs.utils.ActiveMQConstants;

public class ActiveMQSlaveWriter implements SlaveWriter {

	private ActiveMQSession session;
	private BlobStore blobStore;
	
	public ActiveMQSlaveWriter(ActiveMQSession session, BlobStore blobStore) {
		super();
		this.session = session;
		this.blobStore = blobStore;
	}



	@Override
	public void init() {
		try {
						
			Destination broadcastModificationTopic = session.createTopic(ActiveMQConstants.WRITE_TOPIC);
			MessageConsumer broadcastModification = session.createConsumer(broadcastModificationTopic);
			
			broadcastModification.setMessageListener(new MessageListener() {
				
				@Override
				public void onMessage(Message message) {
					try {
						String type = message.getStringProperty("type");
						
						if(type.equals(ActiveMQConstants.REMOVE)) {
							blobStore.remove(message.getStringProperty("key"));
							return;
						} 
						if(type.equals(ActiveMQConstants.INSERT)) {
							BlobMessage blobMessage = (BlobMessage) message;
							String key = blobMessage.getStringProperty("key");
							blobStore.put(key, blobMessage.getInputStream());
							return;
						}
						
						throw new UnsupportedOperationException(type);
						
						
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
					
				
				}
			});
			
		} catch(Throwable t) {
			throw new RuntimeException(t);
		}
		

	}
	
	
	

}
