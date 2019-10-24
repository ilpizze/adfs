package it.antonio.adfs.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BlobStore {

	private final File workingDirectory;
	
	// file system services
	private ExecutorService fileSystemService = Executors.newFixedThreadPool(50);
	private ConcurrentMap<String, Lock> fileLocks = new ConcurrentHashMap<>();
	
	// sync variables
	private AtomicBoolean synching = new AtomicBoolean(); 
	private ConcurrentLinkedQueue<Runnable> waitingToSynchOperations = new ConcurrentLinkedQueue<>(); 
	private ConcurrentLinkedQueue<Runnable> syncOperations = new ConcurrentLinkedQueue<>(); 

	public BlobStore(File workingDirectory) {
		ensureValidWorkingDirectory(workingDirectory);
		this.workingDirectory = workingDirectory;

	}

	private void ensureValidWorkingDirectory(File workingDirectory) {
		if (workingDirectory.isFile()) {
			throw new IllegalStateException(workingDirectory.getAbsolutePath() + " already exists and is a file");
		} else if (!workingDirectory.exists()) {
			if (!workingDirectory.mkdirs()) {
				throw new IllegalStateException("Could not mkdir " + workingDirectory.getAbsolutePath());
			}
		}
	}



	
	
	public void putWriteOperation(String file, WriteOperation op) {
		
		if(synching.get() == true) {
			
			waitingToSynchOperations.add(()-> putWriteOperation(file, op));
			return;
		}
		
		fileSystemService.execute(() -> {
			Lock lock = fileLocks.computeIfAbsent(file, f-> new ReentrantLock(true));
			lock.lock();
			try {
			
				op.execute(file);
				
				
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} finally {
				lock.unlock();
			}
			
		});
	}
	
	
	
	
	
	public InputStream get(String file) {
		try {
			InputStream in = openBlobStream(file);
			return in;
		} catch (FileNotFoundException e) {
			return null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	private InputStream openBlobStream(String key) throws IOException {
		//return new GZIPInputStream(new FileInputStream(new File(workingDirectory, key)));
		return new FileInputStream(new File(workingDirectory, key));
	}


	
	

	
	public void startSync() {
		synching.set(true);
	}
	
	public void putSyncOperation(SynchronizationRunnable runnable) {
			syncOperations.add(runnable);
			
	}
	
	public void flushSyncOperations() {
		try {
			for(File file: workingDirectory.listFiles()) {
				file.delete();
			}
			
			ExecutorService syncService = Executors.newFixedThreadPool(10);
			for(Runnable r: syncOperations) {
				syncService.submit(r);
			}
			
			syncService.shutdown();
			syncService.awaitTermination(24, TimeUnit.HOURS);
			syncOperations.clear();
			
			
			ExecutorService waitingSyncService = Executors.newSingleThreadExecutor();
			
			for(Runnable r: waitingToSynchOperations) {
				waitingSyncService.submit(r);
			}
			
			waitingSyncService.shutdown();
			waitingSyncService.awaitTermination(24, TimeUnit.HOURS);
			
			synching.set(false);
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	
	public abstract class SynchronizationRunnable implements Runnable {
		
		
		
		protected void putDuringSync(String fileName, InputStream stream) {
				
				try {
					File file = new File(workingDirectory, fileName);
					FileOutputStream fileOutputStream = new FileOutputStream(file);
					stream.transferTo(fileOutputStream);
					fileOutputStream.close();
					stream.close();
					
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
				
			
			
		}
	}
	
	
	public interface WriteOperation  {

		void execute(String file) throws IOException;
		
	}
	
	public class InsertDocument implements WriteOperation {
		
		private InputStream stream;
		
		public InsertDocument(InputStream stream) {
			super();
			this.stream = stream;
		}



		@Override
		public void execute(String fileName) throws IOException {
				
				
				File temp = new File(workingDirectory, UUID.randomUUID().toString());
				File file = new File(workingDirectory, fileName);
				FileOutputStream fileOutputStream = new FileOutputStream(temp);
				stream.transferTo(fileOutputStream);
				fileOutputStream.close();
				stream.close();
				
				//final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream);
				//supplier.transferTo(gzipOutputStream);
				temp.renameTo(file);

				
			
		}
		
	}

	public class UpdateDocument implements WriteOperation {
		
		private InputStream stream;
		
		public UpdateDocument(InputStream stream) {
			super();
			this.stream = stream;
		}



		@Override
		public void execute(String fileName) throws IOException {
				
				
				File temp = new File(workingDirectory, UUID.randomUUID().toString());
				File file = new File(workingDirectory, fileName);
				
				FileInputStream fi = new FileInputStream(file);
				Files.copy(fi, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
				fi.close();
				
				
				
				
				FileOutputStream fileOutputStream = new FileOutputStream(temp);
				stream.transferTo(fileOutputStream);
				fileOutputStream.close();
				stream.close();
				
				
				temp.renameTo(file);

				
			
		}
		
	}

	public class RemoveDocument implements WriteOperation {
		
		


		@Override
		public void execute(String fileName) {
				
				
				File blob = new File(workingDirectory, fileName);
				if (blob.exists()) {
					if (!blob.delete()) {
						throw new IllegalStateException("Could not delete " + blob);
					}
				}

				
			
		}
		
	}
	
}