package it.antonio.adfs.utils;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;

public class ActiveMQSessionHolder {
	
	private ActiveMQCreds creds;
	private String fileServerUrl;
	
	private Connection connection;
	private ActiveMQSession session;
	
	
	public ActiveMQSessionHolder(ActiveMQCreds creds, String fileServerUrl) {
		super();
		this.creds = creds;
		this.fileServerUrl = fileServerUrl;
	}
	
	public void init() throws JMSException {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
		connectionFactory.setBrokerURL(creds.url);
		connectionFactory.setUserName(creds.username);
		connectionFactory.setPassword(creds.password);
		
		connectionFactory.getBlobTransferPolicy().setDefaultUploadUrl(fileServerUrl);
		
		
		connection = connectionFactory.createConnection();
		//connection.setClientID(username);
		connection.start();

		session = (ActiveMQSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		
		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				ActiveMQSessionHolder.this.stop();
			}
		});
	}
	
	public void stop() {
		try {
			if(session != null) session.close();
			if(connection != null) connection.close();
			
		}catch(Throwable t) {
			t.printStackTrace();
			throw new RuntimeException(t);
		}
	}
	
	public ActiveMQSession session() {
		return session;
	} 
}
