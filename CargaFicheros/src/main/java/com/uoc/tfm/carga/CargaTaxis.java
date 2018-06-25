package com.uoc.tfm.carga;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.mongodb.spark.MongoSpark;



public class CargaTaxis extends CargaDatos{
	static final String agencia = "TAXI";
	
	public CargaTaxis(){
		//Invoco al constructor de la clase padre con el looger inicialiazon
		//con la clase derivada para que en los logs aparezca el nombre de 
		//la clase derivada y asi poder distinguir quien los ha generado
		//Carga fichero de propiedades
		super(Logger.getLogger(CargaTaxis.class));
		
	}
	
	public static void main(String[] args) {
		//Inicializo una ciudad y un fichero de properties por defecto		
		String ciudad = "BCN";
		String properties = "paradasTaxi.properties";
		
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
		CargaTaxis pt= new CargaTaxis();
		pt.execute(ciudad, properties);	
		
	}
	

	public void execute(String ciudad, String properties){
		logger.info("Inicio carga paradas de taxi");
		
		loadProperties(properties);
		logger.debug("Properties cargadas");
		
		//Descarga del fichero desde la url incluida en el fichero de propiedades
		//y se guarda en el disco.
		//Si el proceso es satisfactorio se continua
		if (cargaFicheros()){
		
			logger.info("Fichero descargado con exito");
			
			// Convierto el fichero en formato XML a JSON	
			if (readXML("/ajax/search/queryresponse/list/list_items/row/item","item" )) {
			
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
					transporteDF.schema();
					//transporteDF.show();	
					
					//Prmera query para filtrar los campos necesarios de la tabla
					String transporteQuery = "SELECT "
								+ "	id.content as cod_parada,"	
		  						+ "	name.content as parada,"						
								+ " addresses.item.district as distrito,"
								+ " addresses.item.barri as barrio,"						
								+ " gmapx.content as lat,"
		  						+ " gmapy.content as lon"
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
					
				    //transporteDF.show();	
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
			logger.error("No ha sido posible descargar el fichero");
		}
		logger.info("Fin carga paradas de taxi");
	}
	
}
