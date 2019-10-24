package it.antonio.adfs.comunication;

import java.io.InputStream;

public interface MasterWriter {

	void init();
	
	void save(String key, InputStream stream);
	
	void remove(String key);

	void append(String fileName, InputStream is);

	
}
