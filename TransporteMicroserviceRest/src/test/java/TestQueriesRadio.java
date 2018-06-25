import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.uoc.tfm.rest.model.TransporteBean;

public class TestQueriesRadio {
	final static Logger logger = Logger.getLogger(TestQueriesRadio.class);
	
	public TestQueriesRadio() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws ParseException {
	
		  String fecha = "2018-05-26T15:39:06.095Z";
		  String latitud = "41.38902";
		  String longitud = "2.172786";
		  String radio = "100";
		  
		 
		  
		  double distance_100 = 0.0012016296386719;
		  double distance_q= distance_100 * Double.parseDouble(radio)/100;
		 
		  double lat=Double.parseDouble(latitud);
		  double lon=Double.parseDouble(longitud);
			 
		  logger.debug("distance_q:" + distance_q);
		  
		  String hora = "";
		  if(fecha.length()>16){	
			  hora = fecha.substring(11, 16) + ":00";
		  }
		  
		  /*public static final int BUSINESS_DAY = 1;
		public static final int SATURDAY_DAY = 2;
		public static final int FESTIVE_DAY = 3;*/
		  
		  String dateType = "1";
		  if(fecha.length()>10){	
			  fecha = fecha = fecha.substring(0, 10);
			  DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			  Date date = format.parse(fecha);
			  Calendar now = Calendar.getInstance();
			  now.setTime(date);
			  int dia = now.get(Calendar.DAY_OF_WEEK);
			  if(now.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY){
				  dateType = "2";
			  }else  if(now.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
				  dateType = "3";
			  }
		  }
	
		MongoClient mongoClient  = null;		
		try{
			mongoClient  = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("TFM");			 
			MongoCollection<Document> collection = db.getCollection("transporte");
			
			BasicDBObject searchQuery = new BasicDBObject();

			//searchQuery.put("ciudad", searchQuery);			 
			searchQuery.put("desde", new BasicDBObject("$lte", hora));
			searchQuery.put("hasta", new BasicDBObject("$gte", hora));	
			searchQuery.put("datetype", dateType);
			
			searchQuery.put("lat",new BasicDBObject("$gte",Double.toString(lat - distance_q)).append("$lt",Double.toString(lat + distance_q)));
			searchQuery.put("lon",new BasicDBObject("$gte",Double.toString(lon - distance_q)).append("$lt",Double.toString(lon + distance_q)));
							 
			FindIterable<Document> resultado = collection.find(searchQuery);
			MongoCursor<Document> cursor = resultado.iterator();			
			try {
				TransporteBean agenda = null;
				while(cursor.hasNext()) {					
					Document d = cursor.next();
					String cod_linea=d.getString("cod_linea");
					String tipo_ruta=d.getString("tipo_ruta");
					if(!"".equals(d.getString("lat")) && !"".equals(d.getString("lon"))){
						double d_lat=Double.parseDouble(d.getString("lat"));
						double d_lon=Double.parseDouble(d.getString("lon"));
						logger.debug("tipo_ruta:" + tipo_ruta + " cod_linea:" + cod_linea + "   "+ Double.toString(Double.parseDouble(latitud)- distance_q)+  " < " + d_lat + " < " + Double.toString(Double.parseDouble(latitud) + distance_q) );
						
						//Calculo la distacia entre las coordenadas del evento y las de la parada de transporte
						double dist = (double) Math.sqrt(
					            Math.pow(d_lat - lat, 2) +
					            Math.pow(d_lon - lon, 2) );
						//Si la distancia es menor o igual añado la parada a la lista
						if(distance_q>=dist){
							logger.debug("distance=" + dist + "<" + distance_q);
						}else{
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
