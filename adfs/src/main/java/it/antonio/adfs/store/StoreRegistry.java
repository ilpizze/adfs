package it.antonio.adfs.store;

import java.util.List;

import it.antonio.adfs.FileEntry;

public interface StoreRegistry {
	
	void init();
	
	void store(String file);
	void remove(String file);

	List<FileEntry> files();
	
	
}
