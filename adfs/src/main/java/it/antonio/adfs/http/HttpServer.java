package it.antonio.adfs.http;

import static spark.Spark.port;
import static spark.Spark.post;
import static spark.Spark.threadPool;
import static spark.Spark.put;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import it.antonio.adfs.comunication.MasterReader;
import it.antonio.adfs.comunication.MasterSync;
import it.antonio.adfs.comunication.MasterWriter;
import it.antonio.adfs.comunication.SlaveRepository;
import it.antonio.adfs.store.StoreRegistry;

import static spark.Spark.*;

import spark.ResponseTransformer;
import spark.utils.IOUtils;

public class HttpServer {
	
	private int port;
	
	private MasterWriter writer;
	private MasterReader reader;
	private MasterSync sync;
	
	private File fileServer;
	
	private StoreRegistry storeRegistry;

	private SlaveRepository slaveRepository;
	
	public HttpServer(StoreRegistry storeRegistry, MasterWriter sender, File fileServer, MasterReader reader, MasterSync sync, SlaveRepository slaveRepository, int port) {
		super();
		this.storeRegistry = storeRegistry;
		this.port = port;
		this.writer = sender;
		this.reader = reader;
		this.sync = sync;
		this.slaveRepository = slaveRepository;
		this.fileServer = fileServer;
	}

	public void init() {
		new Thread(() ->  {
			port(port);
			threadPool(100);
			defaultResponseTransformer(new DefaultResponseTransformer());
			
			enableCORS("*", "GET,POST,PUT,DELETE,HEAD", "*");
			
			post("/files", (request, response) -> {
			    request.attribute("org.eclipse.jetty.multipartConfig", new MultipartConfigElement("/tmp"));
			    byte[] keyBytes = IOUtils.toByteArray(request.raw().getPart("key").getInputStream());
			    String fileName = new String(keyBytes , StandardCharsets.UTF_8);
			    
			    
			    try (InputStream is = request.raw().getPart("file").getInputStream()) {
			    	try {
			    		writer.save(fileName, is);
			    	} catch(Exception e) {
			    		e.printStackTrace();
			    		throw e;
			    	}
			    }
			    
			    storeRegistry.store(fileName);
			    return "ok";
			});
			get("/files/sync/:slave", (request, response) -> {
				String slave = request.params("slave");
				sync.sync(slave);
				return "ok";
			});
			get("/files/slaves", (request, response) -> {
				response.type("application/json");
				List<String> slaves = slaveRepository.slaves();
				return slaves;
			});
			
			get("/files/list", (request, response) -> {
				response.type("application/json");
				return storeRegistry.files();
			});
			get("/files/:fileName", (request, response) -> {
				String key = request.params("fileName");
				
				InputStream stream = reader.read(key);
				if(stream == null) {
					response.status(404);
					return "";
				}
				
				response.raw().setContentType("application/octet-stream");
				response.raw().setHeader("Content-Disposition","attachment; filename="+key);
			    
				ServletOutputStream output = response.raw().getOutputStream();
				
				stream.transferTo(output);
				
				output.flush();
				stream.close();
				
			    return null;
			});
			delete("/files/:fileName", (request, response) -> {
				String fileName = request.params("fileName");
				
				writer.remove(fileName);
				storeRegistry.remove(fileName);
			    
				
			    return null;
			});
			put("/file-server/:id", (request, response) -> {
				ServletInputStream input = request.raw().getInputStream();
				
				String id = request.params("id");
				FileOutputStream output = new FileOutputStream(new File(fileServer, id));
				input.transferTo(output);
				
				output.close();
				input.close();
			    return "ok";
			});
			get("/file-server/:id", (request, response) -> {
				String id = request.params("id");
				
				response.raw().setContentType("application/octet-stream");
				response.raw().setHeader("Content-Disposition","attachment; filename="+id);
			    
				ServletOutputStream output = response.raw().getOutputStream();
				FileInputStream file = new FileInputStream(new File(fileServer, id));
				file.transferTo(output);
				
				output.flush();
				file.close();
				return null;
			});
			
		}).start();
	}
	
	private static void enableCORS(final String origin, final String methods, final String headers) {

	    options("/*", (request, response) -> {

	        String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
	        if (accessControlRequestHeaders != null) {
	            response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
	        }

	        String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
	        if (accessControlRequestMethod != null) {
	            response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
	        }

	        return "OK";
	    });

	    before((request, response) -> {
	        response.header("Access-Control-Allow-Origin", origin);
	        response.header("Access-Control-Request-Method", methods);
	        response.header("Access-Control-Allow-Headers", headers);
	        
	    });
	}
	
	
	private static class DefaultResponseTransformer implements ResponseTransformer {

		private Gson gson = new GsonBuilder().create();
				
		
		@Override
		public String render(Object model) throws Exception {
			if(model instanceof String) {
				return (String) model;
			} else {
				return gson.toJson(model);	
			}
			
		}
		
	}
}
