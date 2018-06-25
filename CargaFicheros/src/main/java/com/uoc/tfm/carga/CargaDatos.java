package com.uoc.tfm.carga;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.uoc.tfm.carga.utils.FileUtils;

public class CargaDatos {
	protected Logger logger;
	
	SimpleDateFormat myFormat = new SimpleDateFormat("yyyyMMdd");

	protected String url = null;
	private String user = null;
	private String pass =  null;
	protected String path = null;
	protected String file =  null;
	protected String fileJSON =  null;
	protected String ciudad =  null;
	protected SparkSession spark = null;
	protected String spark_master =  null;
	protected String spark_appName =  null;
	protected String spark_mongodb_uri =  null;
	
	
	public CargaDatos(Logger logger){
		this.logger=logger;
	}
	
	
	protected static String nodeToString(Node node) throws Exception{
	    StringWriter sw = new StringWriter();
	
	      Transformer t = TransformerFactory.newInstance().newTransformer();
	      t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
	      t.setOutputProperty(OutputKeys.INDENT, "yes");
	      t.transform(new DOMSource(node), new StreamResult(sw));
	
	    return sw.toString();
	  }
	
	protected boolean cargaFicheros() {
		try{
			// Creacion del nuevo directorio
			File newDir = new File(path);
	
			// Si el direcorio no existe se crea
			if (!newDir.exists()) {
				logger.debug("Creando directorio: " + newDir.getName());
				boolean result = false;
				try {
					newDir.mkdir();
					result = true;
				} catch (SecurityException se) {
					logger.error(se);
				}
				if (result) {
					logger.debug("Directorio creado");
				}
			}
			
			// Descarga del ficheo
			logger.debug("Descargando desde la url: " + url);
			if (file.toLowerCase().indexOf(".zip")>-1){
				//El fichero esta comprimido
				if (user != null && pass != null){				
					//Se necesita usuario y contraseña para descargarlo
					FileUtils.downloadFile(url, user, pass, path, file);
				}else{
					FileUtils.downloadURLFile(path, file, url);
				}
				List<String> listFiles = FileUtils.unZip(path, file);
				if(listFiles == null || listFiles.size() == 0){
					//No se ha podido descomprimir el fichero ZIP
					return false;
				}
			}else{				
				FileUtils.downloadURLFile(path, file, url);
			}
			logger.debug("Descargado fichero: " + file + " en la carpeta"+ path);
		}catch (Exception e){
			logger.error(e);
			return false;
		}
		return true;
	}
	
	protected void loadProperties(String filename) {
		Properties prop = new Properties();
		
		InputStream input = null;

		try {

			input = this.getClass().getClassLoader().getResourceAsStream(filename);
			if (input == null) {
				logger.error("Sorry, unable to find " + filename);
				return;
			}

			// load a properties file from class path, inside static method
			prop.load(input);

			// get the property value and print it out
		
			url = prop.getProperty("urlFile");
			if(prop.containsKey("user")){
				user = prop.getProperty("user");
			}
			if(prop.containsKey("pass")){
				pass = prop.getProperty("pass");
			}
			
	    	path = prop.getProperty("path") + myFormat.format(new Date()) + "\\";;
	    	file = prop.getProperty("outFile");

	    	fileJSON = prop.getProperty("outFile_JSON");
	    	ciudad = prop.getProperty("default_city");
	    	spark_master = prop.getProperty("spark_master");
	    	spark_appName = prop.getProperty("spark_appName");
	    	spark_mongodb_uri = prop.getProperty("spark_mongodb_uri");
	  	    	
		} catch (IOException ex) {
			logger.error(ex);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.error(e);
				}
			}
		}
	}
	
	protected boolean readXML(String xmlPath, String nodoDatos) {
		 FileInputStream fis = null;
		 Writer fstream = null;
		  
		 try{
			logger.debug("Inicio lectura del fichero XML ");
			File fXmlFile = new File(path+ file);
			fis = new FileInputStream(fXmlFile);
	
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
			doc.getDocumentElement().normalize();
			XPath xPath = XPathFactory.newInstance().newXPath();
			NodeList nodes = (NodeList) xPath.evaluate(xmlPath, doc.getDocumentElement(), XPathConstants.NODESET);
						 
			StringWriter writer = new StringWriter();
			StreamResult result = new StreamResult(writer);
			  
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			  
			fstream = new OutputStreamWriter(new FileOutputStream(path + fileJSON), StandardCharsets.UTF_8);
			DOMSource source = new DOMSource();
			for (int i = 0; i < nodes.getLength(); ++i) {
			//for (int i = 0; i < 2; ++i) {
			   String nodeString = nodeToString(nodes.item(i));
	
			   JSONObject jsonObject = XML.toJSONObject(nodeString);
		       jsonObject = (JSONObject) jsonObject.get(nodoDatos);
		       logger.debug(jsonObject.toString());
		       
		       //Caso especial apra la carga anual de la agenda
		       if(!"ParadesTaxis.xml".equals(file) && !"bicing.xml".equals(file) 
		    		   && !"PuntsAncoratgeBicicletes.xml".equals(file) && !"Aparcaments.xml".equals(file)){
			      if(jsonObject.has("addresses")){
			    	   JSONObject jsonAddresses=jsonObject.getJSONObject("addresses");
			    	   if(jsonAddresses.has("item")){
			    		   if(jsonAddresses.get("item") instanceof JSONObject){
				    		   JSONObject jsonItem=jsonAddresses.getJSONObject("item");
				    		   jsonObject.put("addresses", jsonItem);
			    		   }else if(jsonAddresses.get("item") instanceof JSONArray){
			    			   JSONArray jsonItem=(JSONArray) jsonAddresses.get("item");
			    			   if(jsonItem.length()>0){
			    				   jsonObject.put("addresses", jsonItem.get(0));
			    			   }
			    		   }
			    	   }
			       }
			       
			       //Caso especial apra la carga de la agenda diaria		      
			      if(jsonObject.has("classificacions") && jsonObject.get("classificacions") instanceof JSONObject){    	  
			    	   JSONObject jsonClassificacions=jsonObject.getJSONObject("classificacions");
			    	   logger.debug(jsonClassificacions.toString());
			    	   if(jsonClassificacions.has("nivell")){
			    		   if(jsonClassificacions.get("nivell") instanceof JSONObject){		    			   
			    			   JSONArray jsonArrayClassif= new JSONArray();
			    			   jsonArrayClassif.put(jsonClassificacions.get("nivell") );
				    		   jsonObject.put("classificacions",jsonArrayClassif );
			    		   }else if(jsonClassificacions.get("nivell") instanceof JSONArray){
			    			   jsonObject.put("classificacions", jsonClassificacions.get("nivell"));
			    		   }
			    	   }
			       }
				}
			   fstream.write(jsonObject.toString()+"\n\r");
			}	      		    			
			return true;
		}catch(Exception e){
			logger.error(e);
			return false;
		}finally{
			if(fis!=null){
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			if(fstream!=null){
				try {
					fstream.flush();
					fstream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}	
		}
		
	}

}
