package it.antonio.adfs;

import java.io.File;
import java.util.Random;

import javax.jms.JMSException;

import it.antonio.adfs.comunication.ActiveMQMasterPinger;
import it.antonio.adfs.comunication.ActiveMQSlaveReader;
import it.antonio.adfs.comunication.ActiveMQSlaveSynch;
import it.antonio.adfs.comunication.ActiveMQSlaveWriter;
import it.antonio.adfs.comunication.MasterPinger;
import it.antonio.adfs.comunication.SlaveReader;
import it.antonio.adfs.comunication.SlaveSync;
import it.antonio.adfs.comunication.SlaveWriter;
import it.antonio.adfs.store.BlobStore;
import it.antonio.adfs.utils.ActiveMQCreds;
import it.antonio.adfs.utils.ActiveMQSessionHolder;

public class Slave {

	
	
	
	
	public static void main(String...args) throws InterruptedException, JMSException {
		
		Random random = new Random();
		int num = random.nextInt(100000);
		String slaveName = "SLAVE_" + num;
		String httpServer = "http://localhost:22344/";
		String url = "failover:tcp://localhost:22334";
		String username = "fs_user";
		String password = "fs_password";
		String folder = "/run/media/antonio/disco2/adfs/f" + num;
		
		new File(folder).mkdirs();
		
		
		ActiveMQCreds creds = new ActiveMQCreds(url, username, password, slaveName);
		
		BlobStore store = new BlobStore(new File(folder));
		
		ActiveMQSessionHolder holder = new ActiveMQSessionHolder(creds, httpServer + "file-server/" );
		holder.init();
		
		
		
		SlaveWriter writer = new ActiveMQSlaveWriter(holder.session(), store );
		writer.init();
		
		SlaveReader reader = new ActiveMQSlaveReader(slaveName, holder.session(), store );
		reader.init();
		
		SlaveSync sync = new ActiveMQSlaveSynch(slaveName, holder.session(), store, httpServer);
		sync.init();
		
		MasterPinger pinger = new ActiveMQMasterPinger(slaveName, holder.session());
		pinger.init();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					holder.stop();
					
				} catch(Exception e) {
					e.printStackTrace();
				}
				
			}
		});
		while(true) Thread.sleep(100000);
	}
	
}
