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

public class CargaTMB extends CargaDatos{

	private SparkSession spark = null;
	private Dataset<Row> calendar_datesDF = null;
	private Dataset<Row> tripsDF = null;
	private Dataset<Row> routesDF = null;
	private Dataset<Row> stopsDF = null;
	private Dataset<Row> stop_timesDF = null;
	private Dataset<Row> calendarDF = null;
	private Dataset<Row> agencyDF = null;		
	private Dataset<Row> frequenciesDF = null;
	SimpleDateFormat myFormat = new SimpleDateFormat("yyyyMMdd");
	

	public static void main(String[] args) {
		//Inicializo una ciudad y un fichero de properties por defecto		
		String ciudad = "BCN";
		String properties = "TMB.properties";
		//String date = "20180514";//Dia laborable
		//String date = "20180512";//Dia sabado
		String date = "20180513";//Dia domingo
				
		//Si se reciben argumentos, el primero se corresponde a la ciudad 
		//el segundo al fichero de propiedades a cargar.
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
		CargaTMB cTMB = new CargaTMB();
		
		
		cTMB.execute(ciudad, properties,date);
		
	}
	
	public CargaTMB(){
		//Invoco al constructor de la clase padre con el looger inicialiazon
		//con la clase derivada para que en los logs aparezca el nombre de 
		//la clase derivada y asi poder distinguir quien los ha generado
		super(Logger.getLogger(CargaTMB.class));
	}

	

	public void execute(String ciudad, String properties, String date  ) {
		
		loadProperties(properties);
		logger.debug("Properties cargadas");
		
		
		//Calculo el tipo de dia de la fecha: laborable, sabado o festivo
		int dayType = calcularDateType(date);
				
		//Descarga del fichero desde la url incluida en el fichero de propiedades
		//y se guarda en el disco.
		//Si el proceso es satisfactorio se continua
		//if (cargaFicheros()){		
		if (true){	//TODO borrar
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
			
				// Calculo las frecuencias del bus
				logger.debug("    Inicio calculo frecuencidas BUS");
				Dataset<Row> frecuenciasDF = null;
				//frecuenciasDF = calcularFrecuenciasBUS(agency, date, dayType, Constantes.ROUTE_BUS);
				frecuenciasDF =spark.read().format("csv").option("sep", ";").option("header", "true").option("encoding", "UTF-8")
					.load("D:\\UOC\\B2.343 - Treball final de màster MIB\\Dades\\tmb\\20180615\\bus_domingo.csv");
				frecuenciasDF = frecuenciasDF.withColumn("tipo_fecha",functions.lit(dayType));
				frecuenciasDF = frecuenciasDF.withColumn("tipo_ruta",functions.lit(Constantes.ROUTE_BUS));	
				
				logger.debug("    Fin calculo frecuencidas BUS");
				
				logger.debug("    Inicio guardado frecuencidas BUS ");
				MongoSpark.save(frecuenciasDF);
				logger.debug("    Fin guardado frecuencidas BUS ");
				
				logger.debug("    Inicio calculo frecuencidas Metro ");
				frecuenciasDF = calcularFrecuenciasMetro(agency, dayType, Constantes.ROUTE_METRO);
				logger.debug("    Fin calculo frecuencidas Metro ");
				
				logger.debug("    Inicio guardado frecuencidas Metro ");
				MongoSpark.save(frecuenciasDF);
				logger.debug("    Fin guardado frecuencidas Metro ");	
				
	
			} catch (Exception e) {
				logger.error(e);
			} finally {
				//Se cierra la session de Spark
				spark.close();
				logger.debug("SparkSession cerrada");
			}
		}else{
			logger.error("No se han podido descargar los ficheros.");
		}

	}
	private Dataset<Row> calcularFrecuenciasMetro(String agency, int dayType, int route_type) {
		String day="";
		if(dayType == Constantes.BUSINESS_DAY){
			 day = " AND calendar.monday=1";
		}else if(dayType == Constantes.SATURDAY_DAY){
			day = " AND calendar.saturday=1";
		}else if(dayType == Constantes.FESTIVE_DAY){
			day = " AND calendar.sunday=1";
		}
		
	
		String queryMetro="SELECT routes.route_id" 
			+ ",routes.route_short_name as cod_linea"
			+ ", routes.route_long_name as nom_linea"				
			+ ", stops.stop_code as cod_parada"
			+ ", stops.stop_name as parada"
			+ ", stops.stop_lat as lat"
			+ " ,stops.stop_lon as lon"
			+ ", stop_times.stop_id"
			+ ", frequencies.start_time as desde"
			+ ", frequencies.end_time as hasta"
			+ ", cast(frequencies.headway_secs as string) as freq"
			//+ ", round((5 * 3600/cast(frequencies.headway_secs as float)),2) as puntos"
			+ "	FROM trips"
			+ " CROSS JOIN calendar ON calendar.service_id = trips.service_id"
			+ " CROSS JOIN routes ON routes.route_id = trips.route_id"
			+ " CROSS JOIN stop_times ON stop_times.trip_id = trips.trip_id"
			+ " CROSS JOIN stops ON stops.stop_id = stop_times.stop_id "
			+ " CROSS JOIN frequencies ON frequencies.trip_id = trips.trip_id "
			+ " WHERE routes.route_type = 1"
			//+ " AND  calendar.saturday=1"
			+ day
			+ " AND  trips.direction_id=1"
			//+ " AND  routes.route_id= '1.3'"
			+ " ORDER BY routes.route_short_name,  Stop_times.stop_sequence DESC , Frequencies.start_time";
			
		Dataset<Row> frecuenciasDF = spark.sql(queryMetro);
		
		frecuenciasDF = frecuenciasDF.withColumn("ciudad",functions.lit(ciudad));
		frecuenciasDF = frecuenciasDF.withColumn("agencia",functions.lit(agency));
		frecuenciasDF = frecuenciasDF.withColumn("tipo_fecha",functions.lit(dayType));
		frecuenciasDF = frecuenciasDF.withColumn("tipo_ruta",functions.lit(route_type));	
			
		frecuenciasDF = frecuenciasDF.drop("route_id")
								.drop("stop_id");
		frecuenciasDF.createOrReplaceTempView("frecuencias_metro");		
		//frecuenciasDF.show();				
		
		//frecuenciasDF.createOrReplaceTempView("frecuencias_metro");
		//frecuenciasDF.coalesce(1).write().option("sep", ",").option("header", "true").option("encoding", "UTF-8").csv(path + "metro_" + dayType +".csv");
	
		return frecuenciasDF;
	}

	private Dataset<Row> calcularFrecuenciasBUS(String agency, String date, int dayType, int route_type) {
		// Calculo hora de inicio y fin de cada linea de bus
		calcularHorarios(date, route_type);
				
		String query = "SELECT "
				+ "				calendar_dates.date "
				+ "				, trips.service_id" 
				+ "				, routes.route_short_name as cod_linea"
				+ "				, routes.route_long_name as nom_linea" 
				+ "				, stop_code as cod_parada"
				+ "				, stops.stop_name  as parada" 
				+ "				, stops.stop_lat  as lat"
				+ "				, stops.stop_lon  as lon"
				+ "				, stop_times.stop_id"
				+ "				, count(stop_times.trip_id) as contador_vacios"
				+ "				, stop_times.arrival_time"
				+ " FROM "
				+ "				trips "
				+ " CROSS JOIN "
				+ "				calendar_dates ON calendar_dates.service_id = trips.service_id"
				+ " CROSS JOIN "
				+ "				routes ON routes.route_id = trips.route_id"
				+ " CROSS JOIN "
				+ "				stop_times ON stop_times.trip_id = trips.trip_id"
				+ " CROSS JOIN "
				+ "				stops ON stops.stop_id = stop_times.stop_id " 
				+ " WHERE "
				+ "		calendar_dates.date = '" + date + "'" 
				+ " 	AND routes.route_type =" + route_type
				//+ " AND routes.route_short_name = '125'"
				//+ " AND (stop_times.stop_id = '2.2493' or stop_times.stop_id= '2.2493')"//2.1116 2.1513
				+ " GROUP BY  	"
				+ "				calendar_dates.date"
				+ "				, trips.service_id"
				+ "				, routes.route_short_name"
				+ "				, routes.route_long_name"
				+ "				, stop_code"
				+ "				, stops.stop_name"
				+ "				, stops.stop_lat"
				+ "				, stops.stop_lon"
				+ "				, stop_times.stop_id"
				+ "				, stop_times.arrival_time"
				+ "				, stop_times.stop_sequence"
				+ " ORDER BY 	"
				+ "				routes.route_short_name"
				+ "				, stop_times.stop_sequence ASC"
				+ "				, stop_times.arrival_time ASC";
		Dataset<Row> frecuenciasDF = spark.sql(query);
		//frecuenciasDF.show();
		
		//Si la hora de llegada es null la inicializo con un -1
		frecuenciasDF = frecuenciasDF.withColumn("arrival_time", functions
				.when(frecuenciasDF.col("arrival_time").isNull(), -1).otherwise(frecuenciasDF.col("arrival_time")));
		
		
		//Inicializo la hora de llegada con la hora de salida del registro anterior
		WindowSpec windowSpecPrev = Window
				.partitionBy(frecuenciasDF.col("date"), frecuenciasDF.col("service_id"),
						frecuenciasDF.col("cod_linea"), frecuenciasDF.col("stop_id"), frecuenciasDF.col("date"))
				.orderBy(frecuenciasDF.col("arrival_time"));
			
		frecuenciasDF = frecuenciasDF.withColumn("departure_time",
				functions.lag("arrival_time", -1, -1).over(windowSpecPrev));
		
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		//frecuenciasDF.show();
	
		
		//Elimino los valores en que los la hora de llegada es igual que la de salida 
		//y le añado la hora de inicio y fin de la linea
		String sqlMerge = "SELECT frecuencias.*, inicio, fin"
				//+ ",cast((duracion/contador_vacios ) as int) as media"
				+ " FROM  frecuencias "
				+ " CROSS JOIN horarios on frecuencias.cod_linea=horarios.route_short_name"
				+ " AND frecuencias.service_id=horarios.service_id"
				+ " WHERE arrival_time <> fin ";
				//+ " AND   arrival_time <> departure_time";
		frecuenciasDF = spark.sql(sqlMerge);
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		//frecuenciasDF.show();
		
		//Las columnas en que la fecha de salida vale -1 la sustituimos por la hora de finalizacion
		// de paso de la linea
		frecuenciasDF = frecuenciasDF.withColumn("departure_time",
							functions.when(frecuenciasDF.col("departure_time").leq(0), frecuenciasDF.col("fin"))
								.otherwise(frecuenciasDF.col("departure_time")));		
		
				
		//Las columnas en que la fecha de llegada vale -1 la sustituimos por la hora de inicio
		// de paso de la linea		
		frecuenciasDF = frecuenciasDF.withColumn("arrival_time",
							functions.when(frecuenciasDF.col("arrival_time").leq(0), frecuenciasDF.col("inicio"))
								.otherwise(frecuenciasDF.col("arrival_time")));		
			
		frecuenciasDF.createOrReplaceTempView("frecuencias");
				
		
		//Calculo la frecuencia de paso es segundos 
		String sql = "SELECT *, (unix_timestamp(departure_time,'HH:mm:ss') - unix_timestamp(arrival_time,'HH:mm:ss')) as frecuencia_paso "
				+ "			FROM  frecuencias";
		frecuenciasDF = spark.sql(sql);
		
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		
		sql = "SELECT *, cast(cast((frecuencia_paso/contador_vacios ) as int) as string)  as freq "
				+ "			FROM  frecuencias";
		frecuenciasDF = spark.sql(sql);
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		
		frecuenciasDF = frecuenciasDF.drop("date")
				.drop("service_id")
				//.drop("stop_id")
				//.drop("contador")
				//.drop("fin")
				//.drop("inicio")
				//.drop("contador_vacios")
				//.drop("frecuencia_paso")
				//.drop("departure_time")
				//.drop("arrival_time")
				;
		
		frecuenciasDF = frecuenciasDF.withColumn("ciudad",functions.lit(ciudad));
		frecuenciasDF = frecuenciasDF.withColumn("agencia",functions.lit(agency));
		frecuenciasDF = frecuenciasDF.withColumn("tipo_fecha",functions.lit(dayType));
		frecuenciasDF = frecuenciasDF.withColumn("tipo_ruta",functions.lit(route_type));	
		frecuenciasDF = frecuenciasDF.withColumnRenamed("arrival_time", "desde");
		frecuenciasDF = frecuenciasDF.withColumnRenamed("departure_time", "hasta");
		
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		
		//Calculo la hora de inicio y fin d cada ruta
		/*String sql = "SELECT *, (unix_timestamp(departure_time,'HH:mm:ss') - unix_timestamp(arrival_time,'HH:mm:ss')) as headway_secs "
				+ "			FROM  frecuencias"
				+ "			WHERE arrival_time <> departure_time"
				+ "			";
		
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		//frecuenciasDF.show();
		String sqlMerge = "SELECT frecuencias.*, fin, inicio"
				// + ", (unix_timestamp(fin,'HH:mm:ss') -
				// unix_timestamp(inicio,'HH:mm:ss')) as duracion "
				+ ",cast((duracion/contador_vacios ) as int) as media"
				+ " FROM  frecuencias "
				+ " CROSS JOIN horarios on frecuencias.cod_linea=horarios.route_short_name"
				+ " AND frecuencias.service_id=horarios.service_id"
				+ " WHERE arrival_time <> fin";
		frecuenciasDF = spark.sql(sqlMerge);
		//frecuenciasDF.show();
/*
		frecuenciasDF = frecuenciasDF.withColumn("headway_secs",
				functions.when(frecuenciasDF.col("headway_secs").isNull(), frecuenciasDF.col("media"))
						.otherwise(frecuenciasDF.col("headway_secs")));

		frecuenciasDF = frecuenciasDF.withColumn("arrival_time",
				functions.when(frecuenciasDF.col("arrival_time").leq(0), frecuenciasDF.col("inicio"))
						.otherwise(frecuenciasDF.col("arrival_time")));
*/
		
		
		
/*
		frecuenciasDF = frecuenciasDF.drop("date")
						.drop("service_id")
						.drop("stop_id")
						.drop("contador")
						.drop("fin")
						.drop("inicio")
						.drop("contador_vacios")
						.drop("media");
		//frecuenciasDF.createOrReplaceTempView("frecuencias");
		
		
		
		
		
		
		frecuenciasDF = frecuenciasDF.withColumnRenamed("arrival_time", "desde");
		frecuenciasDF = frecuenciasDF.withColumnRenamed("departure_time", "hasta");
		frecuenciasDF = frecuenciasDF.withColumn("desde",functions.date_format(frecuenciasDF.col("desde"),"HH.mm"));
		frecuenciasDF = frecuenciasDF.withColumn("hasta",functions.date_format(frecuenciasDF.col("hasta"),"HH.mm"));
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		
		frecuenciasDF = frecuenciasDF.drop("route_id")
									.drop("stop_id");
		
		//String queryPuntos = "SELECT *, round((3600/cast(headway_secs as float)),2) as puntos FROM  frecuencias";
		//frecuenciasDF = spark.sql(queryPuntos);

		frecuenciasDF = frecuenciasDF.withColumnRenamed("headway_secs", "freq");
		
		frecuenciasDF.createOrReplaceTempView("frecuencias");
		//frecuenciasDF.show();
	
		
		frecuenciasDF.coalesce(1).write().option("sep", ";").option("header", "true")
		.option("encoding", "UTF-8").csv(path + "frecuencias_bus_laborable.csv");
		*/
		return frecuenciasDF;
	}

	private void calcularHorarios(String date, int route_type) {

		String query_horarios = "SELECT calendar_dates.date" 
				+ "		, routes.route_short_name"
				+ "		, trips.service_id" 
				+ "		, max(stop_times.arrival_time) as fin"
				+ "		, min(stop_times.arrival_time) as inicio"
				+ " FROM trips "
				+ " CROSS JOIN calendar_dates ON calendar_dates.service_id = trips.service_id"
				+ " CROSS JOIN routes ON routes.route_id = trips.route_id"
				+ " CROSS JOIN stop_times ON stop_times.trip_id = trips.trip_id" 
				+ " WHERE calendar_dates.date = '"	+ date + "'" 
				+ " AND routes.route_type = " + route_type
				// + " AND routes.route_short_name = '62'"
				+ " GROUP BY  calendar_dates.date" 
				+ "			, trips.service_id"
				+ "			, routes.route_short_name" 
				+ " ORDER BY routes.route_short_name ASC";

		Dataset<Row> horariosDF = spark.sql(query_horarios);
		//horariosDF.show();
		horariosDF.createOrReplaceTempView("horarios");

		//horariosDF.coalesce(1).write().option("sep", ";").option("header", "true").option("encoding", "UTF-8")
		//		.csv(path + "horarios.csv");

	}

	private void loadTXTFiles() {
		calendar_datesDF = loadCSVFile(path + "calendar_dates.txt");
		calendar_datesDF.createOrReplaceTempView("calendar_dates");
		//calendar_datesDF.show();

		tripsDF = loadCSVFile(path + "trips.txt");
		tripsDF.createOrReplaceTempView("trips");
		//tripsDF.show();

		routesDF = loadCSVFile(path + "routes.txt");
		routesDF.createOrReplaceTempView("routes");
		//routesDF.show();

		stopsDF = loadCSVFile(path + "stops.txt");
		stopsDF.createOrReplaceTempView("stops");
		//stopsDF.show();

		stop_timesDF = loadCSVFile(path + "stop_times.txt");
		stop_timesDF.createOrReplaceTempView("stop_times");
		//stop_timesDF.show();
		
		calendarDF = loadCSVFile(path + "calendar.txt");
		calendarDF.createOrReplaceTempView("calendar");
		//calendarDF.show();
		
		agencyDF = loadCSVFile(path + "agency.txt");
		agencyDF.createOrReplaceTempView("agency");
		//agencyDF.show();
		
		frequenciesDF = loadCSVFile(path + "frequencies.txt");
		frequenciesDF.createOrReplaceTempView("frequencies");
		//agencyDF.show();
	}

	private Dataset<Row> loadCSVFile(String filename) {
		return spark.read().format("csv").option("sep", ",").option("header", "true").option("encoding", "UTF-8")
				.load(filename);

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

	/*
	 * public void loadFile(String urlFile, String user, String pass){ URL url;
	 * InputStream is = null; BufferedReader br; String line;
	 * 
	 * // Install Authenticator MyAuthenticator.setPasswordAuthentication(user,
	 * pass); Authenticator.setDefault (new MyAuthenticator ());
	 * 
	 * try { url = new URL(urlFile); is = url.openStream(); // throws an
	 * IOException
	 * 
	 * FileOutputStream out = new
	 * FileOutputStream("C:/Development/download.zip"); copy(is, out, 1024);
	 * out.close();
	 * 
	 * } catch (MalformedURLException mue) { mue.printStackTrace(); } catch
	 * (IOException ioe) { ioe.printStackTrace(); } finally { try { if (is !=
	 * null) is.close(); } catch (IOException ioe) { // nothing to see here } }
	 * }
	 */
	
	
}
