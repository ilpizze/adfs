package it.antonio.adfs.store;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.mapdb.DB;
import org.mapdb.DB.HashMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.DBMaker.Maker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import it.antonio.adfs.FileEntry;

public class MapDBStoreRegistry implements StoreRegistry {

	private Maker maker;
	private Gson gson;
	
	public MapDBStoreRegistry(File file) {
		super();
		
		this.gson = new GsonBuilder().create();
		
		this.maker = DBMaker.fileDB(file)
		 					.transactionEnable();
		 					
	}

	@Override
	public void init() {
		DB db = maker.make();
		try {
			entriesMap(db).createOrOpen();
			
			db.commit();
				
		} catch(Exception e){
			
			db.rollback();	
			throw new RuntimeException(e);
		} finally {
			db.close();
		}
		
	}
	
	@Override
	public void store(String fileName) {
		DB db = maker.make();
		try {
			
			HTreeMap<String, String> set = entriesMap(db).open();
			
			FileEntry entry = new FileEntry();
			entry.setName(fileName);
			entry.setDate(new Date());
			
			String entryString = gson.toJson(entry);
			
			set.put(fileName, entryString);
			db.commit();
		} catch(Exception e){
			
			db.rollback();	
			throw new RuntimeException(e);
		} finally {
			db.close();
		}
	}

	@Override
	public void remove(String fileName) {
		DB db = maker.make();
		try {
			
			HTreeMap<String, String> map = entriesMap(db).open();
		
			map.remove(fileName);
			db.commit();
			
		} catch(Exception e){
			
			db.rollback();	
			throw new RuntimeException(e);
		} finally {
			db.close();
		}
	}

	
	@Override
	public List<FileEntry> files() {
		DB db = maker.make();
		try {
			
			HTreeMap<String, String> map = entriesMap(db).open();
			
			List<FileEntry> files = new ArrayList<FileEntry>();
			map.forEach((k,v) -> {
				files.add(gson.fromJson(v, FileEntry.class));
			});
			
			return files;
			
			
		} catch(Exception e){
			
			throw new RuntimeException(e);
		} finally {
			db.close();
		}
	}
	
	private HashMapMaker<String, String> entriesMap(DB db){
		return db.hashMap("files").keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING);
	}
}
