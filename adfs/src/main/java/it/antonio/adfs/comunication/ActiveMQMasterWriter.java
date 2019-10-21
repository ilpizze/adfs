package it.antonio.adfs.comunication;

import java.io.InputStream;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.activemq.ActiveMQSession;

import it.antonio.adfs.utils.ActiveMQConstants;

public class ActiveMQMasterWriter implements MasterWriter{
	
	private ActiveMQSession session;
	private MessageProducer modifications;
	
	public ActiveMQMasterWriter(ActiveMQSession session) {
		super();
		this.session = session;
	}

	@Override
	public void init() {
		try {
			
			
			
			
			Destination broadcastModificationTopic = session.createTopic(ActiveMQConstants.WRITE_TOPIC);
			modifications = session.createProducer(broadcastModificationTopic);

			
			
		}catch(Throwable t) {
			t.printStackTrace();
			throw new RuntimeException(t);
		}
		
		
	}

	
	@Override
	public void save(String key, InputStream stream) {
		try {
			Message message = session.createBlobMessage(stream);
			message.setStringProperty("type", ActiveMQConstants.INSERT);
			message.setStringProperty("key", key);
			
			modifications.send(message);
			
		} catch (JMSException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void remove(String key) {
		try {
			Message message = session.createMessage();
			message.setStringProperty("type", ActiveMQConstants.REMOVE);
			message.setStringProperty("key", key);
			
			modifications.send(message);
			
		} catch (JMSException e) {
			throw new RuntimeException(e);
		}
		
	}

}
