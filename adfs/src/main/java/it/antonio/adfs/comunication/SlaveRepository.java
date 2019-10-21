package it.antonio.adfs.comunication;

import java.util.List;

public interface SlaveRepository {
	void init();

	String randomSlave();

	List<String> slaves();
}
