import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.uoc.tfm.rest.model.EventoBean;
import com.uoc.tfm.rest.model.EventoListsBean;
import com.uoc.tfm.rest.model.SearchBean;

public class TestQueries {
	final static Logger logger = Logger.getLogger(TestQueries.class);
	
	public TestQueries() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws ParseException {
		EventoListsBean eventoRetorno = new EventoListsBean();
		
		SearchBean searchBean = new SearchBean();
		searchBean.setDistrito("Eixample");
		searchBean.setAgenda("0054703001001016");//0054703001001016
		searchBean.setFecha("2018-05-27T11:22:27.648Z");		
		searchBean.setRadio("400");
	
		Calendar calendar = javax.xml.bind.DatatypeConverter.parseDateTime(searchBean.getFecha());
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm");
		simpleDateFormat.setTimeZone(TimeZone.getTimeZone("CET"));
		logger.debug(simpleDateFormat.format(calendar.getTime()));
				
		MongoClient mongoClient  = null;		
		try{
			 mongoClient  = new MongoClient( "localhost" , 27017 );
			 MongoDatabase db = mongoClient.getDatabase("TFM");			 
			 MongoCollection<Document> collection = db.getCollection("eventosDiario");
			 
			 BasicDBObject searchQuery = new BasicDBObject();
			 
			 //Filtro distrito en la query
			 if(searchBean.getDistrito() != null && !"".equals(searchBean.getDistrito())){
				 searchQuery.put("distrito", searchBean.getDistrito());		
				 logger.debug("distrito:" + searchBean.getDistrito());
			 }
			 
			 if(searchBean.getAgenda() != null && !"".equals(searchBean.getAgenda())){
				// searchQuery.put("agenda", searchBean.getAgenda());		
				 logger.debug("agenda:" + searchBean.getAgenda());
				 searchQuery.put("tipus", new BasicDBObject("$in",Arrays.asList(searchBean.getAgenda())));
			 }
			 
			 //Filtro fecha y hora en la query
			 String fecha=searchBean.getFecha();//"2018-05-26T15:39:06.095Z"
			 if(fecha.length()>10){				
				 fecha = fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8,10);
				 logger.debug("fecha:" + fecha);
				 //Rango de fechas
				 searchQuery.put("beginDate", new BasicDBObject("$lte", fecha));
				 searchQuery.put("endDate", new BasicDBObject("$gte", fecha));
				 //"$gte"
				 
				 if(searchBean.getFecha().length()>16){	
					//Rango de horas
					 String hora = searchBean.getFecha().substring(11, 16);
					 hora = hora.replace(":", "");
					 logger.debug("hora:" + hora);
					 
					 searchQuery.put("beginHour", new BasicDBObject("$lte", hora));
					 searchQuery.put("endHour", new BasicDBObject("$gte", hora));				 
				 }
				 /*BasicDBObject query = new BasicDBObject("beginDate",
						   new BasicDBObject("$gte",fecha).append("$lte",fecha ));
				 searchQuery.put(query);*/
			 }
			 
			
			 
			 
			 /*
			 "beginDate" : "01/01/2000"
			 "beginHour" : "00.00"
 				"endDate" : "31/12/9999"
				"endHour" : ""
			 */
			 /*
			 if(searchBean.getAgenda() != null && !"".equals(searchBean.getAgenda())){
				 searchQuery.put("agenda", searchBean.getAgenda());		
				 logger.debug("agenda" + searchBean.getAgenda());
			 }*/
			 
			FindIterable<Document> resultado = collection.find(searchQuery);
			MongoCursor<Document> cursor = resultado.iterator();
			
			try {
				EventoBean agenda = null;
				while(cursor.hasNext()) {					
					 Document d = cursor.next();
					 agenda = new EventoBean();

					 agenda.setIdEvento(d.getString("idEvento"));
					 agenda.setEvento(d.getString("evento"));
					 
					 agenda.setIdCentro(d.getString("idCentro"));
					 agenda.setCentro(d.getString("centro"));
					 agenda.setDireccion(d.getString("direccion"));
					 agenda.setBarrio(d.getString("barrio"));
					 agenda.setDistrito(d.getString("distrito"));
					 
					 agenda.setLat(d.getString("lat"));
					 agenda.setLon(d.getString("lon"));
					 
					 fecha= d.getString("beginDate");
					 if(fecha.length()>7){
						 fecha = fecha.substring(6,8) + "/"+ fecha.substring(4,6) + "/" + fecha.substring(0,4);
						 agenda.setBeginDate(fecha);
					 }
					 fecha= d.getString("endDate");
					 if(fecha.length()>7){
						 fecha = fecha.substring(6,8) + "/"+ fecha.substring(4,6) + "/" + fecha.substring(0,4);
						 agenda.setEndDate(fecha);
					 }
					 
					 String hora = d.getString("beginHour");
					 if(hora.length()>3){
						 hora = fecha.substring(0,2) + ":"+ fecha.substring(2,4);
						 agenda.setBeginHour(hora);
					 }					 
					 hora = d.getString("endHour");
					 if(hora.length()>3){
						 hora = fecha.substring(0,2) + ":"+ fecha.substring(2,4);
						 agenda.setEndHour(hora);
					 }
					 eventoRetorno.addEvento(agenda);					 
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
