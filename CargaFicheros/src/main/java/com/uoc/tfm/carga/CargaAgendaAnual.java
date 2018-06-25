package com.uoc.tfm.carga;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.mongodb.spark.MongoSpark;

public class CargaAgendaAnual  extends CargaDatos{
	//final static Logger logger = Logger.getLogger(CargaAgendaAnual.class);
	
	Properties prop = new Properties();
	
	private SparkSession spark = null;
	
	public CargaAgendaAnual(){
		//Invoco al constructor de la clase padre con el looger inicialiazon
		//con la clase derivada para que en los logs aparezca el nombre de 
		//la clase derivada y asi poder distinguir quien los ha generado
		super(Logger.getLogger(CargaAgendaAnual.class));
		
	}
	
	public static void main(String[] args) {
		//Inicializo una ciudad y un fichero de properties por defecto		
		String ciudad = "BCN";
		String properties = "AgendaAnual.properties";
		
		//Si se reciben argumentos, el primero se corresponde a la ciudad 
		//y el segundo al fichero de propiedades a cargar.
		if (args.length > 0) {
		    try {
		    	ciudad =args[0];		    	
		    	if (args.length >1) {
		    		properties = args[1];
		    	}		    	
		    } catch (Exception e) {
		        System.err.println("Error en los argumentos");
		        System.exit(1);
		    }
		}
		
		//Creo la instancia de la classe y llamo al metodo principal: execute()
		//con los datos de la ciudad y el fichero de propiedades que debe cargar
		CargaAgendaAnual pt= new CargaAgendaAnual();
		pt.execute(ciudad, properties);	
		
	}
	

	public void execute(String ciudad, String properties){
		logger.info("Inicio carga agenda anual");			
	
		loadProperties(properties);
		logger.debug("Properties cargadas");		
		
		//Descarga del fichero desde la url incluida en el fichero de propiedades
		//y se guarda en el disco.
		//Si el proceso es satisfactorio se continua
		if (cargaFicheros()){	
			logger.info("Fichero descargado con exito");
		
			// Convierto el fichero en formato XML a JSON	
			if (readXML("/ajax/search/queryresponse/list/list_items/row/item", "item")) {
			
				try{
					GregorianCalendar gc = new GregorianCalendar();
					gc.setTime(new Date());
					
					gc.set(Calendar.DAY_OF_YEAR, 1);				
					String fecha1Enero = (new SimpleDateFormat("yyyy-MM-dd")).format(gc.getTime());
					
					gc.set(Calendar.DAY_OF_MONTH, 31);
					gc.set(Calendar.MONTH, Calendar.DECEMBER);				
					String fecha31Diciembre = (new SimpleDateFormat("yyyy-MM-dd")).format(gc.getTime());
					
					//Inicio Spark 
					spark = SparkSession.builder().master(spark_master).appName(spark_appName)
							.config("spark.mongodb.input.uri", spark_mongodb_uri)
						    .config("spark.mongodb.output.uri",spark_mongodb_uri)
							.getOrCreate();					
					logger.debug("SparkSession creada");
					
					logger.debug("Inicio creacion del Dataframe ");
					
					//Creacion de un Dataset a partir del fichero JSON
					Dataset<Row> eventosDF =spark.read().option("encoding", "UTF-8").json(path + fileJSON);
					
					//Creacion d la tabla temporal sobre la que ejecutar sentencias SparkSQL
					eventosDF.createOrReplaceTempView("eventos");
					//logger.debug(eventosDF.count());  
					
					//eventosDF.printSchema();
					//eventosDF.show();	
				
					//Prmera query para filtrar los campos necesarios de la tabla
					String eventoSQL = "SELECT "
							+ "	city.content as ciudad,"
							+ "	cast(id.content as long) as idEvento,"
	  						+ "	name.content as nom,"
	  						+ "	cast(equipment_id.content as long) as idCentro,"
	  						+ " institutionname.content as centro,"
	  						+ " address.content as direccion,"
	  						+ " district.content as distrito,"
	  						+ " addresses.barri as barrio,"
	  						+ " addresses.gmapx as lat,"
	  						+ " addresses.gmapy as lon,"
	  						//+ " begindate.content as fecha_inicio,"
	  						+ " TO_DATE(CAST(UNIX_TIMESTAMP(begindate.content, 'dd/MM/yyyy') AS TIMESTAMP)) AS fecha_inicio,"
	  						+ " TO_DATE(CAST(UNIX_TIMESTAMP(enddate.content, 'dd/MM/yyyy') AS TIMESTAMP)) AS fecha_fin"
	  						//+ " enddate.content as fecha_fin"
	  					//	+ " classificacions"
	  						+ " FROM  eventos ";
				
					//Ejecucion de la sentencia y el resultado se guarda en un Dataset
					Dataset<Row> lloc_simpleDF = spark.sql(eventoSQL);
					logger.debug("Se ha ejecutado la query:" + eventoSQL);  
					eventosDF.createOrReplaceTempView("eventos");
					
					//Si no hay fecha de inicio ni fecha de fin se inicializan a 1 de enero y 31 de diciembre respectivametne
					lloc_simpleDF = lloc_simpleDF.withColumn("fecha_inicio",
							functions.when(lloc_simpleDF.col("fecha_inicio").isNull(),fecha1Enero)
									.otherwise(lloc_simpleDF.col("fecha_inicio")));
					
					lloc_simpleDF = lloc_simpleDF.withColumn("fecha_fin",
							functions.when(lloc_simpleDF.col("fecha_fin").isNull(),fecha31Diciembre)
									.otherwise(lloc_simpleDF.col("fecha_fin")));			
					
					//Convierto las fechas a formato YYYYMMDD
					lloc_simpleDF = lloc_simpleDF.withColumn("fecha_inicio",functions.date_format(lloc_simpleDF.col("fecha_inicio"), "yyyyMMdd"));
					lloc_simpleDF = lloc_simpleDF.withColumn("fecha_fin",functions.date_format(lloc_simpleDF.col("fecha_fin"), "yyyyMMdd"));
					
					
					//La hora de inicio y de fin en este fichero no vienen informadas. Las inicializo a las 00:00 y 23:59
					lloc_simpleDF = lloc_simpleDF.withColumn("hora_inicio",functions.lit("00.00"));
					lloc_simpleDF = lloc_simpleDF.withColumn("hora_fin",functions.lit("23.59"));
					
					lloc_simpleDF = lloc_simpleDF.withColumn("ciudad",functions.upper(lloc_simpleDF.col("ciudad")));
					lloc_simpleDF = lloc_simpleDF.withColumn("ciudad",
							functions.when(lloc_simpleDF.col("ciudad").equalTo("BARCELONA"), "BCN")
									.otherwise(lloc_simpleDF.col("ciudad")));
					
					lloc_simpleDF.show();	
					//eventosDF.createOrReplaceTempView("eventos");
					
					//Se persiste el Dataset en la base de datos
					MongoSpark.save(lloc_simpleDF);
					
					logger.debug("Dataframe guardado");
				
				
				}catch(Exception e){
					logger.error(e);
				}finally{
					//Se cierra la session de Spark
					if(spark!=null)
						spark.close();
					logger.debug("SparkSession cerrada");
				}
			}
		}else{
			logger.error("No ha sido posible descargar el fichero");
		}
		logger.info("Fin carga agenda anual");			
	}
	/*
	public List<EventoGenericoBean> readXML(String ciudad, String path, String file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
			
	
		File fXmlFile = new File(path + file);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();

		//System.out.println("Root element :" + doc.getDocumentElement().getNodeName());	
		//NodeList nList = doc.getElementsByTagName("list_items");
		
		List<EventoGenericoBean> listaEventos = new ArrayList<EventoGenericoBean>();
				
		//Evaluate XPath against Document itself
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList) xPath.evaluate("/ajax/search/queryresponse/list/list_items/row/item", doc.getDocumentElement(), XPathConstants.NODESET);
		for (int i = 0; i < nodes.getLength(); ++i) {
		    Element elementoItem = (Element) nodes.item(i);
		    logger.debug(elementoItem.getTagName());
		    NodeList itemChildNodes = elementoItem.getChildNodes();
		    if(itemChildNodes.getLength()>0){
		    	EventoGenericoBean eBean = new EventoGenericoBean(ciudad, itemChildNodes, logger);
		    	listaEventos.add(eBean);    
		    }
		    
		}
		
		return listaEventos;
	}
	*/
	

}
