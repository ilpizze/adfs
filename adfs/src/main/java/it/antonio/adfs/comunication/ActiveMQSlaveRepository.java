package it.antonio.adfs.comunication;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.apache.activemq.ActiveMQSession;

import it.antonio.adfs.utils.ActiveMQConstants;

public class ActiveMQSlaveRepository implements SlaveRepository {

	private static final int SLAVES_MAX_SIZE = 50;
	
	private ActiveMQSession session;
	
	private LinkedList<String> slaves = new LinkedList<String>(); 
	private ReadWriteLock lock = new ReentrantReadWriteLock(); 
	
	public ActiveMQSlaveRepository(ActiveMQSession session) {
		super();
		this.session = session;
	}

	

	@Override
	public void init() {
		try {
						
			Destination pingQueue = session.createQueue(ActiveMQConstants.PING_QUEUE);
			MessageConsumer pingConsumer = session.createConsumer(pingQueue);
			
			pingConsumer.setMessageListener(new MessageListener() {
				
				@Override
				public void onMessage(Message message) {
					try {
						lock.writeLock().lock();
						String slave = message.getStringProperty("slave");
						
						slaves.add(slave);
						if(slaves.size() > SLAVES_MAX_SIZE) {
							slaves.removeFirst(); // remove oldes
						}
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					} finally {
						lock.writeLock().unlock();
					}
					
				
				}
			});
			
		} catch(Throwable t) {
			throw new RuntimeException(t);
		}
		

	}
	
	@Override
	public String randomSlave() {
		try {
			lock.readLock().lock();
			if(slaves.isEmpty()) {
				throw new IllegalArgumentException("No slave pinging");
			}
			return slaves.peekLast(); // recent
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			lock.readLock().unlock();
		}
	}



	@Override
	public List<String> slaves() {
		try {
			lock.readLock().lock();
			if(slaves.isEmpty()) {
				throw new IllegalArgumentException("No slave pinging");
			}
			Set<String> ret = new HashSet<String>();
			slaves.forEach(ret::add);
			return new ArrayList<String>(ret); // recent
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			lock.readLock().unlock();
		}
	}
	

}
