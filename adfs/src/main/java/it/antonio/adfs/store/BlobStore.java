package it.antonio.adfs.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlobStore {

	private final File workingDirectory;

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

	public void put(String key, InputStream supplier) {
		try {
			if(synching.get() == true) {
				
				waitingToSynchOperations.add(()-> put(key, supplier));
				return;
			}
			
			internalPut(key, supplier);
			
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	protected void internalPut(String key, InputStream supplier) throws IOException {

		File temp = new File(workingDirectory, UUID.randomUUID().toString());
		File file = new File(workingDirectory, key);
		FileOutputStream fileOutputStream = new FileOutputStream(temp);
		supplier.transferTo(fileOutputStream);
		fileOutputStream.close();
		
		//final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream);
		//supplier.transferTo(gzipOutputStream);
		temp.renameTo(file);
		
		
	}
	
	/**
	 * Access a blob by key.
	 *
	 * @param key the blob key
	 * @return the input stream to extract the blob data, or if there is no blob for
	 *         the key
	 * @see com.google.common.base.Optional
	 */
	public InputStream get(String key) {
		try {
			InputStream in = openBlobStream(key);
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


	
	public void remove(String key) {
		if(synching.get() == true) {
			
			waitingToSynchOperations.add(()-> remove(key));
			return;
		}
		
		File blob = new File(workingDirectory, key);
		if (blob.exists()) {
			if (!blob.delete()) {
				throw new IllegalStateException("Could not delete " + blob);
			}
		}

	}

	
	public void startSync() {
		synching.set(true);
	}
	
	public void putSyncOperation(SyncRunnable runnable) {
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
	
	
	public abstract class SyncRunnable implements Runnable {
		
		
		
		protected void putDuringSync(String key, InputStream stream) {
				
				try {
					internalPut(key, stream);
					
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
				
			
			
		}
	}
	
}