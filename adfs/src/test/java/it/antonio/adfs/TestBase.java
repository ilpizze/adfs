package it.antonio.adfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;

public class TestBase {

	@Test
	public void test() throws InterruptedException {
		List<String> files = Arrays.asList("f1.txt", "f3.txt", "f2.txt");
		try {
			ExecutorService service = Executors.newFixedThreadPool(200);
			for (int i = 0; i < 10; i++) {
				int  count = i;
				for (String file: files) {
					service.execute(() -> {
	
						try {
							CloseableHttpClient client = HttpClients.createDefault();
							HttpPost httpPost = new HttpPost("http://localhost:22344/files");
	
							MultipartEntityBuilder builder = MultipartEntityBuilder.create();
							builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
							builder.addBinaryBody("file", new ByteArrayInputStream(("prova" + count).getBytes()));
							builder.addTextBody("key", file);
							HttpEntity entity = builder.build();
							httpPost.setEntity(entity);
	
							CloseableHttpResponse response = client.execute(httpPost);
							System.out.println(response.getStatusLine());
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
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
