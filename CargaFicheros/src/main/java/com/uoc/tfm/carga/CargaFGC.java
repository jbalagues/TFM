package com.uoc.tfm.carga;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import com.mongodb.spark.MongoSpark;
import com.uoc.tfm.carga.utils.Constantes;

public class CargaFGC extends CargaDatos{
	private SparkSession spark = null;
	private Dataset<Row> tripsDF = null;
	private Dataset<Row> routesDF = null;
	private Dataset<Row> stopsDF = null;
	private Dataset<Row> stop_timesDF = null;
	private Dataset<Row> calendar_datesDF = null;
	private Dataset<Row> agencyDF = null;	
	
	SimpleDateFormat myFormat = new SimpleDateFormat("yyyyMMdd");
	
	public CargaFGC() {
		//Invoco al constructor de la clase padre con el looger inicialiazon
		//con la clase derivada para que en los logs aparezca el nombre de 
		//la clase derivada y asi poder distinguir quien los ha generado
		super(Logger.getLogger(CargaFGC.class));
		
	}

	public static void main(String[] args) {
		//Inicializo una ciudad y un fichero de properties por defecto		
		String ciudad = "BCN";
		String properties = "FGC.properties";
		
		//String date = "20180515";//Dia laborable
		//String date = "20180519";//Dia sabado
		String date = "20180520";//Dia domingo
		
		//Si se reciben argumentos, el primero se corresponde a la ciudad 
		//y el segundo al fichero de propiedades a cargar.
		//el tercero la fecha en formato AAAAMMDD
		if (args.length > 0) {
		    try {
		    	ciudad =args[0];		    	
		    	if (args.length >1) {
		    		properties = args[1];
		    	}	
		    	if (args.length >2) {
		    		date = args[2];
		    	}	
		    } catch (Exception e) {
		        System.err.println("Error en los argumentos");
		        System.exit(1);
		    }
		}
		//Creo la instancia de la classe y llamo al metodo principal: execute()
		//con los datos de la ciudad, el fichero de propiedades que debe cargar
		//y una fecha de carga 
		CargaFGC cFGC = new CargaFGC();

		cFGC.execute(ciudad, properties, date);
	}
	
	private int calcularDateType(String fecha){
		int dateType = Constantes.BUSINESS_DAY;
    	if(fecha.length()>7){    
    		fecha = fecha.substring(0, 8);
		  	DateFormat format = new SimpleDateFormat("yyyyMMdd");
		  	Date date;
		  	try {
				date = format.parse(fecha);
				Calendar now = Calendar.getInstance();
			  	now.setTime(date);
			   	if(now.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY){
			  		dateType = Constantes.SATURDAY_DAY;
			  	}else  if(now.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
			  		dateType = Constantes.FESTIVE_DAY;
			  	}
			} catch (ParseException e) {
				logger.error(e);
			}
		  	
    	}
    	return dateType;
    }
	public void execute(String ciudad, String properties, String date) {
		
		loadProperties(properties);
		logger.debug("Properties cargadas");
		
		//Descarga del fichero desde la url incluida en el fichero de propiedades
		//y se guarda en el disco.
		//Si el proceso es satisfactorio se continua
		//if (cargaFicheros()){
		if (true) {  //TODO remove
			
			try {
				logger.debug("Inicio ejecucion");
				
				spark = SparkSession.builder().master(spark_master).appName(spark_appName)
						.config("spark.mongodb.input.uri", spark_mongodb_uri)
					    .config("spark.mongodb.output.uri",spark_mongodb_uri)
						.getOrCreate();
				
				logger.debug("SparkSession creada");
	
				// Cargo los ficheros en tablas
				loadTXTFiles();
				logger.debug("Ficheros cargados");
				
				//Recupero el nombre de la entidad de transporte
				String agency = (String) agencyDF.first().get(0);
				logger.debug("La entidad de transporte es "+ agency);
				
				logger.debug("    Inicio calculo frecuencidas FGC");
				Dataset<Row> frecuenciasDF = calcularFrecuenciasFGC(ciudad, agency, date);
				logger.debug("    Fin calculo frecuencidas FGC");
					
				logger.debug("    Inicio guardado frecuencidas FGC ");
				MongoSpark.save(frecuenciasDF);
				logger.debug("    Fin guardado frecuencidas FGC ");	
				
	
			} catch (Exception e) {
				logger.error(e);
			} finally {
				//Se cierra la session de Spark
				if(spark!=null)
					spark.close();
				logger.debug("SparkSession cerrada");
			}
		}else{
			logger.error("No se han podido descargar lo ficheros.");
		}

	}
	private void loadTXTFiles() {	

		tripsDF = loadCSVFile(path + "trips.txt");
		tripsDF.createOrReplaceTempView("trips");
		tripsDF.show();

		stopsDF = loadCSVFile(path + "stops.txt");
		stopsDF.createOrReplaceTempView("stops");
		stopsDF.show();

		stop_timesDF = loadCSVFile(path + "stop_times.txt");
		stop_timesDF.createOrReplaceTempView("stop_times");
		stop_timesDF.show();
		
		agencyDF = loadCSVFile(path + "agency.txt");
		agencyDF.createOrReplaceTempView("agency");
		agencyDF.show();
		
		calendar_datesDF = loadCSVFile(path + "calendar_dates.txt");
		calendar_datesDF.createOrReplaceTempView("calendar_dates");
		calendar_datesDF.show();
		
		routesDF = loadCSVFile(path + "routes.txt");
		routesDF.createOrReplaceTempView("routes");
		routesDF.show();
	}
	
	private Dataset<Row> loadCSVFile(String filename) {
		return spark.read().format("csv").option("sep", ",").option("header", "true").option("encoding", "UTF-8").option("dateFormat", "HH:mm:ss")
				.load(filename);
	}
	
	private Dataset<Row> calcularFrecuenciasFGC(String ciudad, String agency, String date) {
		//Calculo el tipo de dia de la fecha: laborable, sabado o festivo
		int dayType = calcularDateType(date);
		
		String query = "SELECT calendar_dates.date"
				+ ", trips.service_id" 
				+ ", routes.route_short_name  as cod_linea"
				+ ", routes.route_long_name as nom_linea" 
				+ ", stops.stop_name  as parada" 
				+ ", stops.stop_lat  as lat"
				+ ", stops.stop_lon  as lon"
				+ ", stop_times.stop_id"
				+ ", to_timestamp(stop_times.arrival_time,'HH:mm:ss') as desde_fecha"
				+ " FROM trips " 
				+ " CROSS JOIN calendar_dates ON calendar_dates.service_id = trips.service_id"
				+ " CROSS JOIN routes ON routes.route_id = trips.route_id"
				+ " CROSS JOIN stop_times ON stop_times.trip_id = trips.trip_id"
				+ " CROSS JOIN stops ON stops.stop_id = stop_times.stop_id " 
				+ " WHERE calendar_dates.date = '" + date +"'"
				// + " AND routes.route_short_name = 'L6' AND stop_times.stop_id='5'"
				// + " AND (stop_times.stop_id = '2.1116' or stop_times.stop_id=
				// '2.1116')"//2.1116 2.1513
				+ " GROUP BY  calendar_dates.date"
				+ "			, trips.service_id"
				+ "			, routes.route_short_name"
				+ "			, routes.route_long_name"
				+ "			, stops.stop_name"
				+ "			, stops.stop_lat"
				+ "			, stops.stop_lon" 
				+ "			, stop_times.stop_id"
				+ "			, stop_times.arrival_time"
				+ " ORDER BY routes.route_short_name, stop_times.stop_id, desde_fecha ASC";
		Dataset<Row> frecuenciasDF = spark.sql(query);
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		//frecuenciasDF.show();

		WindowSpec windowSpecPrev = Window
				.partitionBy(frecuenciasDF.col("date"), frecuenciasDF.col("service_id"),
						frecuenciasDF.col("cod_linea"), frecuenciasDF.col("stop_id"))
				.orderBy(frecuenciasDF.col("desde_fecha"));

		frecuenciasDF = frecuenciasDF.withColumn("hasta_fecha", functions.lag("desde_fecha", -1, null).over(windowSpecPrev));
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		//frecuenciasDF.show();		
		
		//Calculo la hora de inicio y fin d cada ruta
		String sql = "SELECT *, substring(hasta_fecha,12, 18) as hasta"
				+ ",substring(desde_fecha,12, 18) as desde "
				+ ",cast((unix_timestamp(hasta_fecha,'HH:mm:ss') - unix_timestamp(desde_fecha,'HH:mm;ss')) as string) as freq"
				+ " FROM  frecuencias WHERE desde_fecha IS NOT NULL AND hasta_fecha IS NOT NULL";
		frecuenciasDF = spark.sql(sql);
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		//frecuenciasDF.show();	
		
		//Elimino los segundos de las fechas y cambio el ':' por un '.' para igualarlo al resto de cargas.
		frecuenciasDF = frecuenciasDF.withColumn("desde",functions.date_format(frecuenciasDF.col("desde"),"HH:mm:ss"));
		frecuenciasDF = frecuenciasDF.withColumn("hasta",functions.date_format(frecuenciasDF.col("hasta"),"HH:mm:ss"));
			
		//String queryPuntos = "SELECT *, round((5*3600/cast(headway_secs as float)),2) as puntos FROM  frecuencias";
		//frecuenciasDF = spark.sql(queryPuntos);
		//Elimino las columnas que no son necesarias
		frecuenciasDF = frecuenciasDF.drop("date")
									.drop("service_id")
									.drop("desde_fecha")
									.drop("hasta_fecha");
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		
		//frecuenciasDF = frecuenciasDF.withColumnRenamed("headway_secs", "freq");
		
		frecuenciasDF = frecuenciasDF.withColumn("ciudad",functions.lit(ciudad));
		frecuenciasDF = frecuenciasDF.withColumn("agencia",functions.lit(agency));
		frecuenciasDF = frecuenciasDF.withColumn("tipo_fecha",functions.lit(dayType));
		frecuenciasDF = frecuenciasDF.withColumn("tipo_ruta",functions.lit(Constantes.ROUTE_TREN));	
		//frecuenciasDF.createOrReplaceTempView("frecuencias");
		//frecuenciasDF.show();
		
		//frecuenciasDF.coalesce(1).write().option("sep", ";").option("header", "true").option("encoding", "UTF-8").csv(path + "FGC_festivo.csv");
		
		return frecuenciasDF;
	}

	private void loadMockFiles() {
		/*Dataset<Row> horariosDF = loadCSVFile(
				"D:\\UOC\\B2.343 - Treball final de màster MIB\\Dades\\mocks\\" + "horarios.csv");
		horariosDF.createOrReplaceTempView("horarios");
		horariosDF.show();*/
	
		Dataset<Row> frecuenciasDF = loadCSVFile(
				"D:\\UOC\\B2.343 - Treball final de màster MIB\\Dades\\mocks\\" + "FGC_sabado.csv");
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		frecuenciasDF.show();
	
	}

}
