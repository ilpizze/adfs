package it.antonio.adfs.comunication;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;

import org.apache.activemq.ActiveMQSession;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import it.antonio.adfs.FileEntry;
import it.antonio.adfs.store.BlobStore;

public class ActiveMQSlaveSynch implements SlaveSync {

	private String slaveName;
	private ActiveMQSession session;
	private BlobStore store;

	private Queue slaveQueue;

	private MessageConsumer slaveConsumer;
	private String httpServerUrl;

	public ActiveMQSlaveSynch(String slaveName, ActiveMQSession session, BlobStore store, String httpServerUrl) {
		super();
		this.session = session;
		this.store = store;
		this.slaveName = slaveName;
		this.httpServerUrl = httpServerUrl;
	}

	@Override
	public void init() {
		try {

			slaveQueue = session.createQueue("queue.master." + slaveName + ".sync");
			slaveConsumer = session.createConsumer(slaveQueue);

			slaveConsumer.setMessageListener(new MessageListener() {

				@Override
				public void onMessage(Message message) {

					try {
						
						store.startSync();
						CloseableHttpClient client = HttpClients.createDefault();
						HttpGet httpGet = new HttpGet(httpServerUrl + "/files/list");

						CloseableHttpResponse response = client.execute(httpGet);
						if (response.getStatusLine().getStatusCode() != 200) {
							throw new IllegalStateException(response.getStatusLine().getStatusCode() + " "
									+ response.getStatusLine().getReasonPhrase());
						}

						Gson gson = new GsonBuilder().create();


						Type listType = new TypeToken<ArrayList<FileEntry>>(){}.getType();
						List<FileEntry> files = gson.fromJson(new InputStreamReader(response.getEntity().getContent()),
								listType);

						for (FileEntry f : files) {
							
							
							store.putSyncOperation(store.new SynchronizationRunnable() {
								
								@Override
								public void run() {
									try {
										HttpGet httpGetFile = new HttpGet(httpServerUrl + "files/" + f.getName());
										CloseableHttpResponse fileResponse = client.execute(httpGetFile);
										
										if (fileResponse.getStatusLine().getStatusCode() == 404) {
											// we suppose its deleted
											return;
										}	
										
										if (fileResponse.getStatusLine().getStatusCode() != 200) {
											throw new IllegalStateException(fileResponse.getStatusLine().getStatusCode() + " "
													+ fileResponse.getStatusLine().getReasonPhrase());
										}	
										
										
										putDuringSync(f.getName(), fileResponse.getEntity().getContent());
										
										fileResponse.getEntity().getContent().close();
									} catch(Exception e) {
										e.printStackTrace();
										throw new RuntimeException(e);
									}
									
								}
							});
						}

						store.flushSyncOperations();
						client.close();

					} catch (JMSException | IOException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}

				}
			});

		} catch (Throwable t) {
			t.printStackTrace();
			throw new RuntimeException(t);
		}
	}

}
