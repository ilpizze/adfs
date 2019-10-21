package it.antonio.adfs.comunication;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.activemq.ActiveMQSession;

import it.antonio.adfs.utils.ActiveMQConstants;

public class ActiveMQMasterPinger implements MasterPinger{
	
	private ActiveMQSession session;
	private MessageProducer pingProducer;
	private String name;
	
	public ActiveMQMasterPinger(String name, ActiveMQSession session) {
		super();
		this.name = name;
		this.session = session;
	}

	@Override
	public void init() {
		try {
			
			
			
			
			Destination pingQueue = session.createQueue(ActiveMQConstants.PING_QUEUE);
			pingProducer = session.createProducer(pingQueue);

			new Thread(() -> {
				
					
					
					try {
						while(true) {
							
							Message message = session.createMessage();
							message.setStringProperty("slave", name);
							
							pingProducer.send(message );
							
							Thread.sleep(5000);
						}
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				
				
				
			}).start();
			
		}catch(Throwable t) {
			t.printStackTrace();
			throw new RuntimeException(t);
		}
		
		
	}

	
}
