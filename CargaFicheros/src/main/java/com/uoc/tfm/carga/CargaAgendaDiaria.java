package com.uoc.tfm.carga;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.mongodb.spark.MongoSpark;
public class CargaAgendaDiaria  extends CargaDatos{


	public CargaAgendaDiaria() {
		//Invoco al constructor de la clase padre con el looger inicialiazon
		//con la clase derivada para que en los logs aparezca el nombre de 
		//la clase derivada y asi poder distinguir quien los ha generado
		super(Logger.getLogger(CargaAgendaDiaria.class));
		

	}
	
	
	public static void main(String[] args) {	
		//Inicializo una ciudad y un fichero de properties por defecto		
		String ciudad = "BCN";
		String properties = "AgendaDiaria.properties";
		
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
		CargaAgendaDiaria pt = new CargaAgendaDiaria();
		pt.execute(ciudad, properties);	
	}

	public void execute(String ciudad, String properties) {
		logger.info("Inicio carga agenda diaria");
		
		// Carga fichero de propiedades
		loadProperties(properties);

		//Descarga del fichero desde la url incluida en el fichero de propiedades
		//y se guarda en el disco.
		//Si el proceso es satisfactorio se continua
		if (cargaFicheros()){	
			logger.info("Fichero descargado con exito");

			// Convierto el fichero en formato XML a JSON		
			if (readXML("/response/body/resultat/actes/acte", "acte")) {			
	
				try{
					String fechaHoy = (new SimpleDateFormat("yyyyMMdd")).format(new Date());					
					
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
		  						+ "	eventos.id as idEvento,"
		  						+ "	eventos.nom as nom,"
		  						+ "	lloc_simple.id as idCentro,"
		  						+ " lloc_simple.nom as centro,"
		  						+ " CONCAT(lloc_simple.adreca_simple.carrer.content,' ',lloc_simple.adreca_simple.numero.content ) as direccion,"
		  						+ " lloc_simple.adreca_simple.municipi.content as ciudad,"
		  						+ " lloc_simple.adreca_simple.districte.content as distrito,"
		  						+ " lloc_simple.adreca_simple.barri.content as barrio,"
		  						+ " lloc_simple.adreca_simple.coordenades.googleMaps.lat as lat,"
		  						+ " lloc_simple.adreca_simple.coordenades.googleMaps.lon as lon,"
		  						+ " data.data_proper_acte as fecha_hora_inicio,"
		  						+ " data.hora_fi as hora_fin,"
		  						+ " classificacions.codi"
		  						+ " FROM  eventos "
		  						+ " WHERE eventos.id IS NOT NULL";
					
					//Ejecucion de la sentencia y el resultado se guarda en un Dataset
					Dataset<Row> lloc_simpleDF = spark.sql(eventoSQL);
					logger.debug("Se ha ejecutado la query:" + eventoSQL);  
					//lloc_simpleDF.show();	
					
					//Si la hora de fin no viene informada y su valor es null la inicializo con las 23.59
					lloc_simpleDF = lloc_simpleDF.withColumn("hora_fin",
															functions.when(lloc_simpleDF.col("hora_fin").isNull(), "23.59")
																	.otherwise(lloc_simpleDF.col("hora_fin")));
					//Si la hora de fin no viene informada y su valor es vacoio la inicializo con las 23.59
					lloc_simpleDF = lloc_simpleDF.withColumn("hora_fin",
															functions.when(lloc_simpleDF.col("hora_fin").equalTo(""), "23.59")
																	.otherwise(lloc_simpleDF.col("hora_fin")));
		  
				
							
					//El campo 'hora_inicio tiene el formato: 30/05/2018 08.00
					//Si contiene una hora al final, corto el substring y lo guardo  en 'hora_inicio' 
					//y si no lo inicializo con las 00.00
					lloc_simpleDF = lloc_simpleDF.withColumn("hora_inicio",
							functions.when(lloc_simpleDF.col("fecha_hora_inicio").contains("."), functions.substring(lloc_simpleDF.col("fecha_hora_inicio"), 12,16 ))
							.otherwise("00.00"));
		
					//Si contiene una fecha, corto el substring y lo guardo  en 'fecha_fin' 
					//y si no lo inicializo con las 00.00
					lloc_simpleDF = lloc_simpleDF.withColumn("fecha_fin",
							functions.when(lloc_simpleDF.col("fecha_hora_inicio").contains("/"), functions.substring(lloc_simpleDF.col("fecha_hora_inicio"), 0,10 ))
							.otherwise("00.00"));
					lloc_simpleDF = lloc_simpleDF.withColumn("fecha_fin",functions.lit(fechaHoy));
					lloc_simpleDF = lloc_simpleDF.withColumn("fecha_inicio",functions.lit(fechaHoy));
					lloc_simpleDF = lloc_simpleDF.withColumn("ciudad",functions.lit(ciudad));
					
					lloc_simpleDF = lloc_simpleDF.withColumn("ciudad",functions.upper(lloc_simpleDF.col("ciudad")));
					lloc_simpleDF = lloc_simpleDF.withColumn("ciudad",
							functions.when(lloc_simpleDF.col("ciudad").equalTo("BARCELONA"), "BCN")
									.otherwise(lloc_simpleDF.col("ciudad")));
					
					//El campo 'fecha_hora_inicio' despues de cortarlo ya no es necesario
					lloc_simpleDF.drop("fecha_hora_inicio");
					//lloc_simpleDF.show();	
					
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
		logger.info("Fin carga agenda diaria");
	}
	
	
	/*
	private boolean readXML() {
		 FileInputStream fis = null;
		 Writer fstream = null;
		  
		 try{
			logger.debug("Inicio lectura del fichero XML ");
			File fXmlFile = new File(path+ outFile);
			fis = new FileInputStream(fXmlFile);

			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
			doc.getDocumentElement().normalize();
			XPath xPath = XPathFactory.newInstance().newXPath();
			NodeList nodes = (NodeList) xPath.evaluate("/response/body/resultat/actes/acte", doc.getDocumentElement(), XPathConstants.NODESET);
						 
			StringWriter writer = new StringWriter();
			StreamResult result = new StreamResult(writer);
			  
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			  
			fstream = new OutputStreamWriter(new FileOutputStream(path + outFile_JSON), StandardCharsets.UTF_8);
			DOMSource source = new DOMSource();
			for (int i = 0; i < nodes.getLength(); ++i) {
			//for (int i = 0; i < 2; ++i) {
			   String nodeString = nodeToString(nodes.item(i));

			 	
		       JSONObject jsonObject = XML.toJSONObject(nodeString);
		       jsonObject = (JSONObject) jsonObject.get("acte");
		       
		       logger.debug(jsonObject.toString());
			   fstream.write(jsonObject.toString()+"\n\r");
			}	      		
			logger.debug("Fin lectura del fichero XML ");
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
	
	 private static String nodeToString(Node node) throws Exception{
		    StringWriter sw = new StringWriter();

		      Transformer t = TransformerFactory.newInstance().newTransformer();
		      t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		      t.setOutputProperty(OutputKeys.INDENT, "yes");
		      t.transform(new DOMSource(node), new StreamResult(sw));

		    return sw.toString();
		  }

	private void loadProperties(String filename) {
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
		
			urlFile = prop.getProperty("urlFile");
	    	path = prop.getProperty("path") + myFormat.format(new Date()) + "\\";;
	    	outFile = prop.getProperty("outFile");

	    	outFile_JSON = prop.getProperty("outFile_JSON");
	    	default_city = prop.getProperty("default_city");
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

	private void cargaFicheros() {
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
		logger.debug("Conectado a: " + urlFile);
		FileUtils.downloadURLFile(path, outFile, urlFile);
		
		logger.debug("Descargado fichero: " + outFile + " en la carpeta"+ path);
	}*/
}
