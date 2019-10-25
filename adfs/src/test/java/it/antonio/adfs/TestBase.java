package it.antonio.adfs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;

public class TestBase {

	@Test
	public void test() throws InterruptedException {
		List<String> files = Arrays.asList("f1.dat", "f3.dat"/*,, "f2.dat", "f2.dat"*/);
		try {
			ExecutorService service = Executors.newFixedThreadPool(200);
			for (int i = 0; i < 1; i++) {
				int  count = i;
				for (String file: files) {
					service.execute(() -> {
	
						try {
							CloseableHttpClient client = HttpClients.createDefault();
							HttpPost httpPost = new HttpPost("http://localhost:22344/files");
	
							MultipartEntityBuilder builder = MultipartEntityBuilder.create();
							builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
							//builder.addBinaryBody("file", new FileInputStream("/run/media/antonio/disco2/nlp/pos-aciapetti/it-train-perceptron.pos"), ContentType.APPLICATION_OCTET_STREAM, file);
							builder.addPart("file", new InputStreamBody(new FileInputStream("/run/media/antonio/disco2/nlp/pos-aciapetti/it-train-perceptron.pos"), file));
							builder.addTextBody("key", file);
							HttpEntity entity = builder.build();
							httpPost.setEntity(entity);
	
							CloseableHttpResponse response = client.execute(httpPost);
							//System.out.println(response.getStatusLine());
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	
					});
				
					Thread.sleep(30);
				}
			}

			service.shutdown();
			service.awaitTermination(30, TimeUnit.MINUTES);
			
			for(String file: files) {
				
			CloseableHttpClient client = HttpClients.createDefault();
			HttpGet httpGet = new HttpGet("http://localhost:22344/files/" + file);

			

			CloseableHttpResponse response = client.execute(httpGet);
			
		
			FileOutputStream ou = new FileOutputStream(new File(file));
			response.getEntity().getContent().transferTo(ou);
			response.getEntity().getContent().close();
			ou.close();
			}
			/*
			
			
			*/
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
