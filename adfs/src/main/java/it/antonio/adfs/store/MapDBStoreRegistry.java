package it.antonio.adfs.store;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.mapdb.DB;
import org.mapdb.DB.HashMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import it.antonio.adfs.FileEntry;

public class MapDBStoreRegistry implements StoreRegistry {

	private DB db;
	private Gson gson;
	
	public MapDBStoreRegistry(File file) {
		super();
		
		this.gson = new GsonBuilder().create();
		
		this.db = DBMaker.fileDB(file)
							.closeOnJvmShutdown()
		 					.transactionEnable()
		 					.make();
		 					
	}

	@Override
	public void init() {
		try {
			entriesMap().createOrOpen();
			
			db.commit();
				
		} catch(Exception e){
			
			db.rollback();	
			throw new RuntimeException(e);
		} 
		
	}
	
	@Override
	public void store(String fileName) {
		try {
			HTreeMap<String, String> set = entriesMap().open();
			
			FileEntry entry = new FileEntry();
			entry.setName(fileName);
			entry.setDate(new Date());
			
			String entryString = gson.toJson(entry);
			
			set.put(fileName, entryString);
			db.commit();
			
		} catch(Exception e){
			
			db.rollback();	
			throw new RuntimeException(e);
		}
	}

	@Override
	public void remove(String fileName) {
		try {
			
			HTreeMap<String, String> map = entriesMap().open();
		
			map.remove(fileName);
			db.commit();
			
		} catch(Exception e){
			
			db.rollback();	
			throw new RuntimeException(e);
		} 
	}

	
	@Override
	public List<FileEntry> files() {
		try {
			
			HTreeMap<String, String> map = entriesMap().open();
			
			List<FileEntry> files = new ArrayList<FileEntry>();
			map.forEach((k,v) -> {
				files.add(gson.fromJson(v, FileEntry.class));
			});
			
			return files;
			
			
		} catch(Exception e){
			
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public boolean contains(String filename) {
		try {
			
			HTreeMap<String, String> map = entriesMap().open();
			
			
			return map.containsKey(filename);
			
			
		} catch(Exception e){
			
			throw new RuntimeException(e);
		}
	}
	
	private HashMapMaker<String, String> entriesMap(){
		return db.hashMap("files").keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING);
	}
}
