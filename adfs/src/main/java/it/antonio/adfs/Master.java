package it.antonio.adfs;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;

import it.antonio.adfs.comunication.ActiveMQMasterReader;
import it.antonio.adfs.comunication.ActiveMQMasterSync;
import it.antonio.adfs.comunication.ActiveMQMasterWriter;
import it.antonio.adfs.comunication.ActiveMQSlaveRepository;
import it.antonio.adfs.comunication.MasterReader;
import it.antonio.adfs.comunication.MasterSync;
import it.antonio.adfs.comunication.MasterWriter;
import it.antonio.adfs.comunication.SlaveRepository;
import it.antonio.adfs.http.FileServerCleaner;
import it.antonio.adfs.http.HttpServer;
import it.antonio.adfs.store.MapDBStoreRegistry;
import it.antonio.adfs.store.StoreRegistry;
import it.antonio.adfs.utils.ActiveMQCreds;
import it.antonio.adfs.utils.ActiveMQSessionHolder;

public class Master {

	
	
	
	
	public static void main(String...args) throws Exception {
		
		int httpPort = 22344;
		String httpServer = "http://localhost:22344/";
		String brokerUrl = "tcp://localhost:22334";
		
		String url = "failover:tcp://localhost:22334";
		String username = "fs_user";
		String password = "fs_password";
		String fileServerPath = "/run/media/antonio/disco2/adfs/fileServer";
		String masterPath = "/run/media/antonio/disco2/adfs/master";
		
		
		List<AuthenticationUser> users = new ArrayList<>();
		users.add(new AuthenticationUser(username,  password, "users"));

		

		SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);
		
		BrokerService broker = new BrokerService();
		broker.addConnector(brokerUrl);
		broker.setPlugins(new BrokerPlugin[] { authenticationPlugin });
		broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
		broker.setPersistent(false);
		
		broker.start();
		
		
		ActiveMQCreds creds = new ActiveMQCreds(url, username, password, "MASTER");
		
		
		ActiveMQSessionHolder holder = new ActiveMQSessionHolder(creds, httpServer + "file-server/");
		holder.init();
		
		SlaveRepository slaveRepository = new ActiveMQSlaveRepository(holder.session());
		slaveRepository.init();
		
		MasterWriter sender = new ActiveMQMasterWriter(holder.session());
		sender.init();
		
		MasterReader reader = new ActiveMQMasterReader(holder.session(), slaveRepository );
		reader.init();
		
		MasterSync sync = new ActiveMQMasterSync(holder.session());
		sync.init();
		
		
		File fileServer = new File(fileServerPath);
		if(!fileServer.exists()) {
			fileServer.mkdirs();
		}
		
		File masterFile = new File(masterPath);
		if(!masterFile.exists()) {
			masterFile.mkdirs();
		}
		
		StoreRegistry registry = new MapDBStoreRegistry(new File(masterFile, "registry.dat"));
		registry.init();
		
		HttpServer server = new HttpServer(registry, sender, fileServer, reader, sync, slaveRepository, httpPort);
		server.init();
		
		FileServerCleaner fs = new FileServerCleaner(fileServer);
		fs.init();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					broker.stop();

					server.close();
					
					holder.stop();
					
				} catch(Exception e) {
					e.printStackTrace();
				}
				
			}
		});
		
		while(true) Thread.sleep(100000);
		
	}
	
}
