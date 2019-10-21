package it.antonio.adfs.utils;

public class ActiveMQCreds {
	public String url;
	public String username;
	public String password;
	public String peerName;
	
	public ActiveMQCreds(String url, String username, String password, String peerName) {
		super();
		this.url = url;
		this.username = username;
		this.password = password;
		this.peerName = peerName;
	}	
	
}
