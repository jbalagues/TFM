import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.uoc.tfm.rest.model.EventoBean;
import com.uoc.tfm.rest.model.EventoListsBean;
import com.uoc.tfm.rest.model.SearchBean;

public class TestQueriesRadio {
	final static Logger logger = Logger.getLogger(TestQueriesRadio.class);
	
	public TestQueriesRadio() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws ParseException {
	
		  String fecha = "2018-05-26T15:39:06.095Z";
		  String latitud = "41.38902";
		  String longitud = "2.172786";
		  String radio = "300";

		  
		  double distance_100 = 0.0012016296386719;
		  double distance_q= distance_100 * Double.parseDouble(radio)/100;
		 
		  double lat=Double.parseDouble(latitud);
		  double lon=Double.parseDouble(longitud);
			 
		  logger.debug("distance_q:" + distance_q);
		  
		  String hora = "2018-06-04T07:33:29.989Z";
		  if(hora == null || "".equals(hora)){			  
			  Calendar calendar = new GregorianCalendar();
			  calendar.setTimeInMillis((new Date().getTime()));
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");
				simpleDateFormat.setTimeZone(TimeZone.getTimeZone("CET"));
				logger.debug(simpleDateFormat.format(calendar.getTime()));
				hora = simpleDateFormat.format(calendar.getTime());
		  }else{	
			  if(fecha.length()>16){	
				  hora = fecha.substring(11, 16) + ":00";
			  }
		  }
		  /*public static final int BUSINESS_DAY = 1;
		public static final int SATURDAY_DAY = 2;
		public static final int FESTIVE_DAY = 3;*/
		  
		  int dateType = 1;
		  if(fecha.length()>10){	
			  fecha = fecha = fecha.substring(0, 10);
			  DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			  Date date = format.parse(fecha);
			  Calendar now = Calendar.getInstance();
			  now.setTime(date);
			  int dia = now.get(Calendar.DAY_OF_WEEK);
			  if(now.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY){
				  dateType = 2;
			  }else  if(now.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
				  dateType = 3;
			  }
		  }
	
		MongoClient mongoClient  = null;		
		try{
			mongoClient  = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("TFM");			 
			MongoCollection<Document> collection = db.getCollection("transporte");
			
			BasicDBObject searchQuery = new BasicDBObject();

			searchQuery.put("ciudad", "BCN");	
			searchQuery.put("tipo_fecha", dateType);
			
			BasicDBList or = new BasicDBList();		
			
			BasicDBObject searchQueryHoraMetro = new BasicDBObject();
			searchQueryHoraMetro.put("desde", new BasicDBObject("$lte", hora));
			searchQueryHoraMetro.put("hasta", new BasicDBObject("$gte", hora));	
			or.add(searchQueryHoraMetro);
			
			BasicDBObject searchQueryHoraBUS = new BasicDBObject();
			searchQueryHoraBUS.put("arrival_time", new BasicDBObject("$gte", hora));	
			searchQueryHoraBUS.put("departure_time", new BasicDBObject("$lte", hora));			
			or.add(searchQueryHoraBUS);
			
			searchQuery.put("$or", or);
		
			
			
			searchQuery.put("lat",new BasicDBObject("$gte",Double.toString(lat - distance_q)).append("$lte",Double.toString(lat + distance_q)));
			searchQuery.put("lon",new BasicDBObject("$gte",Double.toString(lon - distance_q)).append("$lte",Double.toString(lon + distance_q)));
			
			logger.debug(searchQuery);	 
			FindIterable<Document> resultado = collection.find(searchQuery);
			MongoCursor<Document> cursor = resultado.iterator();			
			try {
				EventoBean agenda = null;
				while(cursor.hasNext()) {					
					Document d = cursor.next();
					String cod_linea=d.getString("cod_linea");
					String tipo_ruta=d.getInteger("tipo_ruta").toString();
					if(!"".equals(d.getString("lat")) && !"".equals(d.getString("lon"))){
						double d_lat=Double.parseDouble(d.getString("lat"));
						double d_lon=Double.parseDouble(d.getString("lon"));
						//logger.debug("tipo_ruta:" + tipo_ruta + " cod_linea:" + cod_linea + "   "+ Double.toString(Double.parseDouble(latitud)- distance_q)+  " < " + d_lat + " < " + Double.toString(Double.parseDouble(latitud) + distance_q) );
						
						//Calculo la distacia entre las coordenadas del evento y las de la parada de transporte
						double dist = (double) Math.sqrt(
					            Math.pow(d_lat - lat, 2) +
					            Math.pow(d_lon - lon, 2) );
						//Si la distancia es menor o igual añado la parada a la lista
						if(distance_q>=dist){
							logger.debug(d);
						}else{
							logger.debug(d);
							logger.debug("distance=" + dist);
						}
					}
				}	 						 
			}catch (Exception e){
				logger.error(e);
			} finally {
				  cursor.close();
			}
	
		}catch (Exception e){
			logger.error(e);
			
		}finally{
			if(mongoClient !=null){
				mongoClient.close();
			}			
			logger.debug("Fin postPerson");
		}

	}

}
