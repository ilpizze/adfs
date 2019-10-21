package it.antonio.adfs.comunication;

import java.io.InputStream;

public interface MasterReader {
	void init();
	public InputStream read(String key);
}
