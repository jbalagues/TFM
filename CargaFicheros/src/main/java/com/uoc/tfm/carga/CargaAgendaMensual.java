package com.uoc.tfm.carga;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.uoc.tfm.beans.EventoGenericoBean;
import com.uoc.tfm.beans.EventoMensualBean;
import com.uoc.tfm.beans.TransporteBean;
import com.uoc.tfm.carga.utils.Constantes;



public class CargaAgendaMensual {
	final static Logger logger = Logger.getLogger(CargaAgendaMensual.class);
	
	Properties prop = new Properties();
	
	private SparkSession spark = null;
	
	public CargaAgendaMensual(){
		//Carga fichero de propiedades
				loadProperties("AgendaMensual.properties");
				logger.debug("Properties cargadas");
	}
	
	public static void main(String[] args) {
		 
		CargaAgendaMensual pt= new CargaAgendaMensual();
		pt.execute("BCN");
		
	}
	

	public void execute(String ciudad){
		logger.info("Inicio carga agenda mensual");
				
		//Carga la lista de enventos anuales
		List<EventoMensualBean> listaEventoMensual = null;
		try {
			listaEventoMensual = readXML(ciudad,  prop.getProperty("path"), prop.getProperty("file_mensual1"));
			 logger.debug("Se han cargado " + listaEventoMensual.size() + " eventos");			 
		} catch (Exception e) {
			logger.error("Error carga agenda mensual:" + e.getMessage());
		}	
		
		if(listaEventoMensual!=null && listaEventoMensual.size()>0){
			
			spark = SparkSession.builder().master("local").appName("MongoSparkConnectorETL")
					// .config("spark.mongodb.input.uri",
					// "mongodb://127.0.0.1/test.myCollection")
					// .config("spark.mongodb.output.uri",
					// "mongodb://127.0.0.1/test.myCollection")
					.getOrCreate();
			
			 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
			
			 //Como las paradas estan activas tanto dias laborables, sabados como festivos, hay que inicializar el campo 'datetype'
			 
			 
			 JavaRDD<EventoMensualBean> paradasRDD = jsc.parallelize(listaEventoMensual);
			 logger.debug("Spark paradasRDD creado ");
			 
			 
			 jsc.close();
		}
	}
	
	public List<EventoMensualBean> readXML(String ciudad, String path, String file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
			
	
		File fXmlFile = new File(path + file);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();

		//System.out.println("Root element :" + doc.getDocumentElement().getNodeName());	
		//NodeList nList = doc.getElementsByTagName("list_items");
		
		List<EventoMensualBean> listaEventos = new ArrayList<EventoMensualBean>();
				
		//Evaluate XPath against Document itself
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList) xPath.evaluate("/response/body/resultat/actes/acte", doc.getDocumentElement(), XPathConstants.NODESET);
		for (int i = 0; i < nodes.getLength(); ++i) {
		    Element elementoItem = (Element) nodes.item(i);
		    logger.debug(elementoItem.getTagName());
		    NodeList itemChildNodes = elementoItem.getChildNodes();
		    if(itemChildNodes.getLength()>0){
		    	EventoMensualBean eBean = new EventoMensualBean(ciudad, itemChildNodes, logger);
		    	listaEventos.add(eBean);    
		    }
		    
		}
		
		return listaEventos;
	}
	
	public void loadProperties(String filename) {

		 InputStream input = null;
   	
   	try {
       
   		input = this.getClass().getClassLoader().getResourceAsStream(filename);
   		if(input==null){
   	        logger.error("Sorry, unable to find " + filename);
   		    return;
   		}

   		//load a properties file from class path, inside static method
   		prop.load(input);

           //get the property value and print it out
   		logger.debug(prop.getProperty("path"));
   		logger.debug(prop.getProperty("file"));

   	} catch (IOException ex) {
   		logger.error(ex);
       } finally{
       	if(input!=null){
       		try {
					input.close();
				} catch (IOException e) {
					logger.error(e);
				}
       	}
       }
  	  }

}
