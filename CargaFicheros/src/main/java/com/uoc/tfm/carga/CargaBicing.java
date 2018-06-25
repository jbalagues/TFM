package com.uoc.tfm.carga;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.mongodb.spark.MongoSpark;



public class CargaBicing  extends CargaDatos{
	
	static final String agencia = "BICING";
	
	public CargaBicing(){
		//Invoco al constructor de la clase padre con el looger inicialiazon
		//con la clase derivada para que en los logs aparezca el nombre de 
		//la clase derivada y asi poder distinguir quien los ha generado
		super(Logger.getLogger(CargaBicing.class));
		
	}
	
	public static void main(String[] args) {	
		//Inicializo una ciudad y un fichero de properties por defecto		
		String ciudad = "BCN";
		String properties = "bicing.properties";
		
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
		CargaBicing pt= new CargaBicing();
		pt.execute(ciudad, properties);		
	}	

	public void execute(String ciudad, String properties){
		logger.info("Inicio carga estaciones de bicing");
				
		loadProperties(properties);
		logger.debug("Properties cargadas");
		
		//Descarga del fichero desde la url incluida en el fichero de propiedades
		//y se guarda en el disco
		//Si el proceso es satisfactorio se continua
		if(cargaFicheros()){

			logger.info("Fichero descargado con exito");
			
			//Convierto el fichero XML a JSON
			if (readXML("/bicing_stations/station", "station")) {

				try{
				
					//Inicio Spark 
					spark = SparkSession.builder().master(spark_master).appName(spark_appName)
							.config("spark.mongodb.input.uri", spark_mongodb_uri)
						    .config("spark.mongodb.output.uri",spark_mongodb_uri)
							.getOrCreate();
					logger.debug("SparkSession creada");
					
					logger.debug("Inicio creacion del Dataframe ");
					
					//Creacion de un Dataset a partir del fichero JSON
					Dataset<Row> transporteDF =spark.read().option("encoding", "UTF-8").json(path + fileJSON);
					
					//Creacion d la tabla temporal sobre la que ejecutar sentencias SparkSQL
					transporteDF.createOrReplaceTempView("transporte");
					//logger.debug(transporteDF.count());  
					
					//transporteDF.printSchema();
					//transporteDF.schema();
					//transporteDF.show();	
					
					//Prmera query para filtrar los campos necesarios de la tabla
					String transporteQuery = "SELECT "
								+ "	cast(id as String)  as cod_parada,"	
		  						+ "	CONCAT(street, ' ', streetNumber ) as parada,"						
								+ " cast(lat as String) as lat,"
		  						+ " cast(long as String)  as lon"
								+ " FROM  transporte ";
					//Ejecucion de la sentencia y el resultado se guarda en un Dataset
					transporteDF = spark.sql(transporteQuery);
					logger.debug("Se ha ejecutado la query:" + transporteQuery);  
					//transporteDF.show();
				
					//Se añaden las columnas no recibidas con sus valores pord efecto
					transporteDF = transporteDF.withColumn("ciudad",functions.lit(ciudad));
					transporteDF = transporteDF.withColumn("agencia",functions.lit(agencia));
					transporteDF = transporteDF.withColumn("tipo_fecha",functions.lit(4));
					
					transporteDF = transporteDF.withColumn("tipo_ruta",functions.lit(""));
					transporteDF = transporteDF.withColumn("cod_linea",functions.lit(""));
					transporteDF = transporteDF.withColumn("nom_linea",functions.lit(""));
					transporteDF = transporteDF.withColumn("tipo_fecha",functions.lit(4));
					transporteDF = transporteDF.withColumn("desde",functions.lit("00:00:00"));
					transporteDF = transporteDF.withColumn("hasta",functions.lit("23:59:00"));
					transporteDF = transporteDF.withColumn("distrito",functions.lit(""));
					transporteDF = transporteDF.withColumn("barrio",functions.lit(""));
					//Se persiste el Dataset en la base de datos
					MongoSpark.save(transporteDF);
					
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
			logger.error("No se han podido descargar los ficheros.");
		}
		logger.info("Fin carga estaciones de bicing");
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
			NodeList nodes = (NodeList) xPath.evaluate("/bicing_stations/station", doc.getDocumentElement(), XPathConstants.NODESET);
						 
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
		       jsonObject = (JSONObject) jsonObject.get("station");
		       
		       logger.debug(jsonObject.toString());
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
	
	public List<TransporteBean> readXMLw(String ciudad, String path, String file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
			
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		
		Calendar c = Calendar.getInstance();
		c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		String fechaInicio = sdf.format(c.getTime());         
		
		c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		String fechaFin= sdf.format(c.getTime()); 
		
		File fXmlFile = new File(path + file);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();

		//System.out.println("Root element :" + doc.getDocumentElement().getNodeName());	
		//NodeList nList = doc.getElementsByTagName("list_items");
		
		List<TransporteBean> listaParadas = new ArrayList<TransporteBean>();
		TransporteBean ptBean = null;
		
		//Evaluate XPath against Document itself
		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodes = (NodeList) xPath.evaluate("/bicing_stations/station", doc.getDocumentElement(), XPathConstants.NODESET);
		for (int i = 0; i < nodes.getLength(); i++) {
		    Element eRow = (Element) nodes.item(i);
		    NodeList nodesRow = eRow.getChildNodes();
		    if(nodesRow.getLength()>0){
		    	ptBean =  new TransporteBean();
		    	ptBean.setAgency("BICING");	
		    	ptBean.setCiudad(ciudad);
		    	ptBean.setDesde("00:00");
		    	ptBean.setHasta("23:59");
		    	ptBean.setCod_parada("");
		    	ptBean.setNom_parada("");
		    	ptBean.setFrec(Constantes.DEFAULT_FREC);
		    	ptBean.setPuntuacion(Constantes.PUNTUACION_BICING);
		    	for (int j = 0; j < nodesRow.getLength(); j++) {
		    		if(nodesRow.item(j) instanceof Element){
		    			Element eItem = (Element) nodesRow.item(j);
		    			NodeList nodesValues = eItem.getChildNodes();
		    			String name=eItem.getNodeName();
		    			
		    			String value = "";
		    			if(nodesValues.item(0)!=null && nodesValues.item(0).getNodeValue()!=null){
		    				 value = nodesValues.item(0).getNodeValue();
		    			}
		    			
		    			if("id".equals(name)){
		    				ptBean.setId(value);	
		    			}else if("lat".equals(name)){
		    				ptBean.setLat(value);
		    			}else if("lat".equals(name)){
		    				ptBean.setLon(value);
		    			}else if("long".equals(name)){
		    				ptBean.setCod_linea("");
		    				ptBean.setNom_linea(value);
		    			}else if("street".equals(name)){	
		    				ptBean.setCod_linea("");
		    				ptBean.setNom_linea(value);
		    				ptBean.setBarrio("");
		    				ptBean.setDistrito("");
		    			}	    			
		    			
				    	logger.debug(name + "-" + value);
		    		}
			    	
		    	}	
		    	listaParadas.add(ptBean);		    	
		    }
		}
		
		return listaParadas;
	}*/
}
