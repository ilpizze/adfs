package it.antonio.adfs.http;

import java.io.File;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileServerCleaner {

	private File fileServer;
	
	public FileServerCleaner(File fileServer) {
		super();
		this.fileServer = fileServer;
	}



	public void init() {
		
		ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
		
		Runnable command = ()-> {
			File[] files = fileServer.listFiles();
			
			for(File file: files) {
				
				long diff = new Date().getTime() - file.lastModified();
				if (diff >  3 * 60 *  60 * 1000) { // 3 hour
				    file.delete();
				}
			}
		};
		service.scheduleAtFixedRate(command , 0, 1, TimeUnit.MINUTES);
	}
}
