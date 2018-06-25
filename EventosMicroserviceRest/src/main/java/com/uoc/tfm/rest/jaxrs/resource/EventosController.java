package com.uoc.tfm.rest.jaxrs.resource;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

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

@Path("/search")
public class EventosController {
	final static Logger logger = Logger.getLogger(EventosController.class);	

    
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON}) 
    @Path("/events")
    public EventoListsBean searchEvents(SearchBean searchBean) throws Exception{
    	logger.debug("Inicio postBusqueda");
    	
    	/*searchBean = new SearchBean();
		searchBean.setDistrito("Eixample");
		searchBean.setAgenda("0054703001001016");//0054703001001016
		searchBean.setFecha("2018-05-26T15:39:06.095Z");	
		searchBean.setHora("2018-05-27T16:01:27.648Z");	
		searchBean.setRadio("400");
		*/
		
    	String fecha=searchBean.getFecha();
    	String hora=searchBean.getHora();
    	
    	EventoListsBean eventoRetorno = new EventoListsBean();
    
    	MongoClient mongoClient  = null;		
		try{
			 mongoClient  = new MongoClient( "localhost" , 27017 );
			 MongoDatabase db = mongoClient.getDatabase("TFM");			 
			 MongoCollection<Document> collection = db.getCollection("eventos");
			 
			 BasicDBObject searchQuery = searchAgendaDiaria(searchBean);		 
			 FindIterable<Document> resultado = collection.find(searchQuery);
			 
			 logger.debug(searchQuery);
			
			 if(!resultado.iterator().hasNext()){
				 collection = db.getCollection("eventosAnual");
				 searchQuery = searchAgendaAnual(searchBean);	
				 resultado = collection.find(searchQuery);
				 logger.debug(searchQuery);
			 }
			 
			 MongoCursor<Document> cursor = resultado.iterator();			 
			 EventoBean agenda = null;
			 while(cursor.hasNext()) {	
				 try {
					 Document d = cursor.next();
					 
					 boolean estaAgenda= false;
					 if(searchBean.getAgenda() != null && !"".equals(searchBean.getAgenda()) && d.containsKey("nom") ){
						 
						 Document ds = (Document) d.get("classificacions");
						 if(ds!=null && ds.containsKey("nivell")){
							 String nivell = ds.getString("nivell");
							 String cadena = "codi\":\"" + searchBean.getAgenda() + "\"";
							 if(nivell.indexOf(cadena)>-1){
								 logger.debug("TROBAT!!!");
								 estaAgenda= true;
							 }
							 logger.debug(ds.getString("nivell"));
						 }
					 }else{
						 estaAgenda= true;
					 }
									 
					 //.get("nivell");
					 logger.debug(d.toJson());
					 
					 //Si el evento no tiene nombre, latitud y longitud no podemos ubicarlo
					 agenda = new EventoBean();
					 
					 if(d.containsKey("idEvento")){	
						 
						 agenda.setIdEvento(d.getLong("idEvento").toString());
					 }else{
						 estaAgenda = false;
					 }
					 
					 if(d.containsKey("nom")){
						 agenda.setEvento(d.getString("nom"));
					 }else{
						 estaAgenda = false;
					 }
					
					 if(d.containsKey("idCentro"))	
						 agenda.setIdCentro(d.getLong("idCentro").toString());
					 
					 if(d.containsKey("centro"))	
						 agenda.setCentro(d.getString("centro"));
					
					 if(d.containsKey("direccion"))	
						 agenda.setDireccion(d.getString("direccion"));
					 
					 if(d.containsKey("barrio"))	
						 agenda.setBarrio(d.getString("barrio"));
					 
					 if(d.containsKey("distrito"))	
						 agenda.setDistrito(d.getString("distrito"));
					
					 if(d.containsKey("lat")){
						 agenda.setLat(d.getString("lat"));
					 }else{
						 estaAgenda = false;
					 }
					 
					 if(d.containsKey("lon")){
						 agenda.setLon(d.getString("lon"));
					 }else{
						 estaAgenda = false;
					 }
					 
					 if(d.containsKey("fecha_inicio"))	 {
						 fecha= d.getString("fecha_inicio");
						 if(fecha!=null && fecha.length()>7){
							 fecha = fecha.substring(6,8) + "/"+ fecha.substring(4,6) + "/" + fecha.substring(0,4);
							 agenda.setBeginDate(fecha);
						 }
					 }
					 if(d.containsKey("fecha_fin"))	 {
						 fecha= d.getString("fecha_fin");
						 if(fecha!=null && fecha.length()>7){
							 fecha = fecha.substring(6,8) + "/"+ fecha.substring(4,6) + "/" + fecha.substring(0,4);
							 agenda.setEndDate(fecha);
						 }
					 }
					 if(d.containsKey("hora_inicio"))	 {
						 hora = d.getString("hora_inicio");
						 if(hora!=null && hora.length()>3 && !"0000".equals(hora)){
							 hora = hora.substring(0,2) + ":"+ hora.substring(2,4);
							 agenda.setBeginHour(hora);
						 }else{
							 agenda.setBeginHour("");
						 }
					 }
					 if(d.containsKey("hora_fin"))	 {
						 hora = d.getString("hora_fin");
						 if(hora!=null && hora.length()>3 && !"2359".equals(hora)){
							 hora = hora.substring(0,2) + ":"+ hora.substring(2,4);
							 agenda.setEndHour(hora);
						 }else{
							 agenda.setEndHour("");
						 }
					 }	
					 if(estaAgenda){
						 eventoRetorno.addEvento(agenda);	
					 }			 
								 
				}catch (Exception e){
					logger.error(e);
				} finally {
				
				}
			}	
			cursor.close();
		}catch (Exception e){			
			logger.error(e);
		}finally{
			if(mongoClient !=null){
				mongoClient.close();
			}			
			logger.debug("Fin postBusqueda");
		}
     
		
		return eventoRetorno;
    }
    
    private BasicDBObject searchAgendaDiaria(SearchBean searchBean){
    	BasicDBObject searchQuery = new BasicDBObject();
    	
    	//Filtro distrito en la query
		 if(searchBean.getDistrito() != null && !"".equals(searchBean.getDistrito())){
			 searchQuery.put("distrito", searchBean.getDistrito());		
			 logger.debug("distrito:" + searchBean.getDistrito());
		 }
		 
		 if(searchBean.getAgenda() != null && !"".equals(searchBean.getAgenda())){
			// searchQuery.put("agenda", searchBean.getAgenda());		
			 logger.debug("agenda:" + searchBean.getAgenda());
			 searchQuery.put("codi", new BasicDBObject("$in",Arrays.asList(searchBean.getAgenda())));
		 }
		 
		 //Filtro fecha y hora en la query
		 String fecha=searchBean.getFecha();//"2018-05-26T15:39:06.095Z"
		 if(fecha!= null && fecha.length()>10){				
			 fecha = fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8,10);
			 logger.debug("fecha:" + fecha);
			 //Rango de fechas
			 searchQuery.put("fecha_inicio", new BasicDBObject("$lte", fecha));
			 searchQuery.put("fecha_fin", new BasicDBObject("$gte", fecha));
		 }	
		 String hora =searchBean.getHora();
		 if(hora!= null && hora.length()>10){	
			 Calendar calendar = javax.xml.bind.DatatypeConverter.parseDateTime(searchBean.getHora());
			 SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH.mm");
			 simpleDateFormat.setTimeZone(TimeZone.getTimeZone("CET"));
			 hora = simpleDateFormat.format(calendar.getTime()); //HH:mm
			 logger.debug("hora:" + hora);
			 searchQuery.put("hora_inicio", new BasicDBObject("$lte", hora));
			 searchQuery.put("hora_fin", new BasicDBObject("$gte", hora));		
		 }
		 return searchQuery;
    }
    
    private BasicDBObject searchAgendaAnual(SearchBean searchBean){
    	BasicDBObject searchQuery = new BasicDBObject();
    	
    	//Filtro distrito en la query
		 if(searchBean.getDistrito() != null && !"".equals(searchBean.getDistrito())){
			 searchQuery.put("distrito", searchBean.getDistrito());		
			 logger.debug("distrito:" + searchBean.getDistrito());
		 }
		 
		 if(searchBean.getAgenda() != null && !"".equals(searchBean.getAgenda())){
			// searchQuery.put("agenda", searchBean.getAgenda());		
			 logger.debug("agenda:" + searchBean.getAgenda());
			 searchQuery.put("codi", new BasicDBObject("$in",Arrays.asList(searchBean.getAgenda())));
		 }
		 
		 //Filtro fecha y hora en la query
		 String fecha=searchBean.getFecha();//"2018-05-26T15:39:06.095Z"
		 if(fecha!= null && fecha.length()>10){				
			 fecha = fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8,10);
			 logger.debug("fecha:" + fecha);
			 //Rango de fechas
			 searchQuery.put("fecha_inicio", new BasicDBObject("$lte", fecha));
			 searchQuery.put("fecha_fin", new BasicDBObject("$gte", fecha));
		 }	

		 return searchQuery;
    }
    
   /* @GET
    @Path("{latitud}/{longitud}/{radio}/{fecha}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.TEXT_PLAIN})
    public String getTransporte(@PathParam("latitud") String latitud,
    							@PathParam("longitud") String longitud,
    							@PathParam("radio") String radio,
    							@PathParam("fecha") String fecha)  throws Exception{*/
    
    /*    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    //@Produces({MediaType.APPLICATION_JSON}) 
    @Produces({MediaType.TEXT_PLAIN})
    @Path("/transports")
  public String postEvento(SearchTransporteBean searchBean) throws Exception{  
    	logger.debug("Inicio postEvento");
    	    	
    	logger.debug("radioe = "+searchBean.getRadio());
    	logger.debug("latitud = "+searchBean.getLat());
    	logger.debug("longitud = "+searchBean.getLon());
    	logger.debug("fecha = "+searchBean.getFecha());
    	logger.debug("hora = "+searchBean.getHora());
		  
    	double distance_100 = 0.0012016296386719;
		double distance_q= distance_100 * Double.parseDouble(searchBean.getRadio())/100;
		 
		double lat=Double.parseDouble(searchBean.getLat());
		double lon=Double.parseDouble(searchBean.getLon());
		  
    	MongoClient mongoClient  = null;		
		try{
			 mongoClient  = new MongoClient( "localhost" , 27017 );
			 MongoDatabase db = mongoClient.getDatabase("TFM");			 
			 MongoCollection<Document> collection = db.getCollection("transporte");
			 
			 BasicDBObject searchQuery = new BasicDBObject();
			 
			 
			 searchQuery.put("lat",new BasicDBObject("$gte",Double.toString(lat - distance_q)).append("$lt",Double.toString(lat + distance_q)));
			 searchQuery.put("lon",new BasicDBObject("$gte",Double.toString(lon - distance_q)).append("$lt",Double.toString(lon + distance_q)));
				
			 searchQuery.put("datetype", calcularDateType(searchBean.getFecha()));
	
			 String hora = parseHoraSegundos(searchBean.getHora());
			 searchQuery.put("desde", new BasicDBObject("$lte", hora));
			 searchQuery.put("hasta", new BasicDBObject("$gte", hora));	
			 
			 
			 FindIterable<Document> resultado = collection.find(searchQuery);
			 MongoCursor<Document> cursor = resultado.iterator();
			 try {
					AgendaBean agenda = null;
					while(cursor.hasNext()) {		
						Document d = cursor.next();
						logger.debug(d.toJson());
					}
			}catch (Exception e){
				logger.error(e);
			} finally {
				  cursor.close();
			}
			
			 cursor.close();
		}catch (Exception e){
			logger.error(e);
			
		}finally{
			if(mongoClient !=null){
				mongoClient.close();
			}			
			logger.debug("Fin postEvento");
		}
		
      /* String retorno = " {"
        	+ "\"agendas\": ["
        	+ "		{\"id\":\"40001029\", \"name\":\"Campanyes\"},"
        	+ "		{\"id\":\"40001002\", \"name\":\"Cinema i projeccions (circuit no comercial)\"},"
        	+ " 	]"
        	+ "}";*/
      /* String retorno = " { "
		    		   + "\"coordenadas\":["
			    		   + "{\"cod_linea\":\"L2\",\"nom_linea\":\"Paral·lel - Badalona Pompeu Fabra\",\"cod_parada\":\"216\",\"parada\":\"Sagrada Família\",\"lat\":\"41.403985\", \"lon\":\"2.175106\",\"puntos\":\"75.0\",\"agency\":\"TMB\",\"datetype\":\"1\",\"tipo_ruta\":\"1\"},"
			    		   + "{\"cod_linea\":\"L2\",\"nom_linea\":\"Paral·lel - Badalona Pompeu Fabra\",\"cod_parada\":\"215\",\"parada\":\"Sagrada Família\",\"lat\":\"41.400524\",\"lon\":\"2.179462\",\"puntos\":\"100.0\",\"agency\":\"TMB\",\"datetype\":\"1\",\"tipo_ruta\":\"1\"},"
			    		   + "{\"cod_linea\":\"L4\",\"nom_linea\":\"La Pau - Trinitat Nova\",\"cod_parada\":\"427\",\"parada\":\"Verdaguer\",\"lat\":\"41.400365\", \"lon\":\"2.16806\",\"puntos\":\"60.0\",\"agency\":\"TMB\",\"datetype\":\"1\",\"tipo_ruta\":\"1\"},"
			    		   + "{\"cod_linea\":\"L5\",\"nom_linea\":\"Cornellà  Centre - Vall d'Hebron\",\"cod_parada\":\"522\",\"parada\":\"Verdaguer\",\"lat\":\"41.398952\", \"lon\":\"2.167089\",\"puntos\":\"75.0\",\"agency\":\"TMB\",\"datetype\":\"1\",\"tipo_ruta\":\"3\"}"
			    		   + "	]"
		    		   + "}";
        return retorno;
    }*/
    
   
}
