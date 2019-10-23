package it.antonio.adfs.comunication;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.apache.activemq.ActiveMQSession;

import it.antonio.adfs.utils.ActiveMQConstants;

public class ActiveMQSlaveRepository implements SlaveRepository {

	private static final int SLAVES_MAX_SIZE = 100;
	private static final Duration SLAVES_MAX_WAIT = Duration.ofSeconds(30); // 30 seconds
	
	private ActiveMQSession session;
	
	private LinkedList<String> slavePings = new LinkedList<String>(); 
	private Map<String, Date> slaveLastPing = new HashMap<>();
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
						
						slavePings.add(slave);
						slaveLastPing.put(slave, new Date());
						
						if(slavePings.size() > SLAVES_MAX_SIZE) {
							slavePings.removeFirst(); // remove oldes
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
			if(slavePings.isEmpty()) {
				throw new IllegalArgumentException("No slave pinging");
			}
			return slavePings.peekLast(); // recent
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
			
			Set<String> ret = slaveLastPing.keySet().stream()
					.filter(slave -> {
						return slaveLastPing.get(slave).toInstant().isAfter(Instant.now().minus(SLAVES_MAX_WAIT) );
					}).collect(Collectors.toSet());
			
			return new ArrayList<String>(ret); // recent
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			lock.readLock().unlock();
		}
	}
	

}
