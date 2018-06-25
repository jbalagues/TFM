package com.uoc.tfm.rest.jaxrs.resource;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.uoc.tfm.rest.model.SearchBean;
import com.uoc.tfm.rest.model.TransporteBean;
import com.uoc.tfm.rest.model.TransporteListBean;


@Path("/search")
public class TransporteController {
	final static Logger logger = Logger.getLogger(TransporteController.class);
	private double distance_100 = 0.0012016296386719;

    
    @SuppressWarnings("deprecation")
	@POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON}) 
    //@Produces({MediaType.TEXT_PLAIN})
    @Path("/transports")
    public TransporteListBean searchTransports(SearchBean searchBean) throws Exception{  
    	logger.debug("Inicio postEvento");
    	    	
    	logger.debug("radio = "+searchBean.getRadio());
    	logger.debug("latitud = "+searchBean.getLat());
    	logger.debug("longitud = "+searchBean.getLon());
    	logger.debug("fecha = "+searchBean.getFecha());
    	logger.debug("hora = "+searchBean.getHora());
		  
    	
    	TransporteListBean transporteListBean = new TransporteListBean();    	
		List<TransporteBean> transportes = new ArrayList<TransporteBean>();
		
    	MongoClient mongoClient  = null;		
		try{
						
			double lat=Double.parseDouble(searchBean.getLat());
			double lon=Double.parseDouble(searchBean.getLon());
			//double distance_q= distance_100 * Double.parseDouble(searchBean.getRadio())/100;
			double radioM=Double.parseDouble(searchBean.getRadio());
			
			mongoClient  = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("TFM");			 
			MongoCollection<Document> collection = db.getCollection("transporte");
			 
			BasicDBObject searchQuery = prepararQuery(searchBean);
			logger.debug("searchQuery:" + searchQuery.toJson());
			FindIterable<Document> resultado = collection.find(searchQuery);
			MongoCursor<Document> cursor = resultado.iterator();			
			
				
			while(cursor.hasNext()) {	
				try {
					boolean dentroRadio=false;
					Document d = cursor.next();
				
					double dist = 0;
					//Filtramos las latitudes y logintudes por distancia
					if(!"".equals(d.getString("lat")) && !"".equals(d.getString("lon"))){
						double d_lat=Double.parseDouble(d.getString("lat"));
						double d_lon=Double.parseDouble(d.getString("lon"));
						//logger.debug("tipo_ruta:" + tipo_ruta + " cod_linea:" + cod_linea + "   "+ Double.toString(Double.parseDouble(latitud)- distance_q)+  " < " + d_lat + " < " + Double.toString(Double.parseDouble(latitud) + distance_q) );
						
						//Calculo la distacia entre las coordenadas del evento y las de la parada de transporte
						/*dist = (double) Math.sqrt(
					            Math.pow(d_lat - lat, 2) +
					            Math.pow(d_lon - lon, 2) );
						*/
						dist = getDistancia(lat,lon, d_lat, d_lon);
						logger.debug("dist=" + dist + "  --radioKM=" + radioM);
						//Si la distancia es menor o igual añado la parada a la lista
						if(radioM>=dist){						
							logger.debug(d);
							dentroRadio=true;
						}else{
							logger.debug("distance=" + dist);
						}
					}
					
					if(dentroRadio){
						TransporteBean transporte = new TransporteBean();
						
						transporte.setCiudad(d.getString("ciudad"));
						transporte.setAgencia(d.getString("agencia"));
						
						if(d.containsKey("tipo_ruta") && d.get("tipo_ruta") instanceof Integer ) {
							transporte.setTipo_ruta(d.getInteger("tipo_ruta").toString());
						}
						if(d.containsKey("cod_linea")) {
							transporte.setCod_linea(d.getString("cod_linea"));
						}
						if(d.containsKey("nom_linea")) {
							transporte.setNom_linea(StringEscapeUtils.unescapeHtml4(d.getString("nom_linea")));
						}
						if(d.containsKey("cod_parada")) {
							transporte.setCod_parada(d.getString("cod_parada"));
						}
						if(d.containsKey("parada")) {
							transporte.setParada(StringEscapeUtils.unescapeHtml4(d.getString("parada")));
						}
						transporte.setLat(d.getString("lat"));
						transporte.setLon(d.getString("lon"));
						
											
						if(!d.containsKey("freq") || "".equals(d.getString("freq")) ){
							//Si no hay frecuencia solo tengo en cuenta la distancia
							transporte.setPuntos(getCapacidad(transporte) * calcularDistancia(Math.abs(dist)));							
						}else{
							transporte.setFreq(d.getString("freq"));
							transporte.setPuntos(calcularPuntuacion(transporte, Math.abs(dist)));
						}	
						transporte.setDistancia(String.valueOf((int) dist));
						transportes.add(transporte);
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
			transporteListBean.setTransportes(transportes);
			
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
		    		   + "}";*/
        return transporteListBean;
    }
    
    private double calcularPuntuacion(TransporteBean trasporte, double dist){
    	try{
	    	double frequencia = 0;
	    	//F=(-x)/36+100
	    	double frequenciaPaso = Double.parseDouble(trasporte.getFreq());
	    	frequenciaPaso = Math.abs(frequenciaPaso);
	    	if(frequenciaPaso>3600) {
	    		frequencia = 10;
	    	}else{
	    		frequencia = 100 - (frequenciaPaso/36);
	    	}
	    	//Capacidad
	    	double capacidad = getCapacidad(trasporte);
    		
	    	//(D=(-9x)/200+100)
	    	double distancia = 0;
	    	//dist = dist * 100 / distance_100;	    	
	    	distancia  =  calcularDistancia(dist);// 0.1 - (0.9*dist/2000);
	    	//double distance_q= distance_100 * Double.parseDouble(searchBean.getRadio())/100;
	    	logger.debug(trasporte.getNom_linea() + " frequencia=" + frequencia + " capacidad=" + capacidad + "  distancia=" + distancia);
	    	//Punto transporte =C*F+D=C*((-x)/36+100)+  (-9x)/200+100	    	
	    	return new Double(frequencia * capacidad * distancia);
	    	
    	}catch(Exception e){
    		return 1;
    	}
    }
    
    private double calcularDistancia(double distancia){
    	return 1 - (0.9*distancia/2000);
    }
    
    private double getCapacidad(TransporteBean trasporte){
    	double capacidad = 1;	   
    	//logger.debug(trasporte.getAgencia()+ "- " + trasporte.getNom_linea() + "- " + trasporte.getParada());
    	switch(trasporte.getAgencia()){
    		case "TMB": 
    			int tipoRuta = Integer.parseInt(trasporte.getTipo_ruta());
    			if(tipoRuta == 1){
    				//Metro
    				capacidad = 5;
    			}else{
    				capacidad = 1;
    			}    			
    			break;
    		case "FGC": //FGC
    			capacidad = 5;
    			break;
    		case "BICING":
    			capacidad = 0.3;
    			break;
    		case "PARKING_BICI": 
    			capacidad = 0.3;
    			break;
    		case "TAXI":
    			capacidad = 1;
    			break;	
    		case "PARKING":
    			capacidad = 1;
    			break;			    			
    	}
    	return capacidad;    	
    }
    
   /* private String parseHoraSegundos(String hora){
    	if(hora.length()>10){	
			 Calendar calendar = javax.xml.bind.DatatypeConverter.parseDateTime(hora);
			 SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");
			 simpleDateFormat.setTimeZone(TimeZone.getTimeZone("CET"));
			 hora = simpleDateFormat.format(calendar.getTime()); //HH:mm
			 logger.debug("hora:" + hora);
		 }
    	return hora;
    }*/
    
   /* private String calcularDateType(String fecha){
    	String dateType = "1";
    	if(fecha.length()>10){    

		  	DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		  	Date date;
		  	try {
				date = format.parse(fecha);
				Calendar now = Calendar.getInstance();
			  	now.setTime(date);
			  	if(now.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY){
			  		dateType = "2";
			  	}else  if(now.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
			  		dateType = "3";
			  	}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  	
    	}
    	return dateType;
    }*/
    
    
    
    private BasicDBObject prepararQuery(SearchBean searchBean) throws ParseException{
    	   	 
    	
    	double distance_q= distance_100 * Double.parseDouble(searchBean.getRadio())/100;
	 
    	double lat=Double.parseDouble(searchBean.getLat());
    	double lon=Double.parseDouble(searchBean.getLon());
		 
    	logger.debug("distance_q:" + distance_q);
	  
    	String hora = searchBean.getHora();
    	if(hora == null || "".equals(hora)){			  
    		Calendar calendar = new GregorianCalendar();
    		calendar.setTimeInMillis((new Date().getTime()));
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");
			simpleDateFormat.setTimeZone(TimeZone.getTimeZone("CET"));
			logger.debug(simpleDateFormat.format(calendar.getTime()));
			hora = simpleDateFormat.format(calendar.getTime());
    	}else{	
    		if(searchBean.getHora().length()>16){	
    			hora = searchBean.getHora().substring(11, 16) + ":00";
    		}
    	}
    	int dateType = 1;
    	String fecha = searchBean.getFecha();
    	if(fecha.length()>10){	

    		DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    		Date date = format.parse(fecha);
    		Calendar now = Calendar.getInstance();
    		now.setTime(date);
    		
		 	if(now.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY){
		 		dateType = 2;
		 	}else  if(now.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
			  dateType = 3;
		 	}
    	}
    	
    	BasicDBObject searchQuery = new BasicDBObject();
    	searchQuery.put("ciudad", searchBean.getCiudad());	
    	//searchQuery.put("agencia", "TMB");	 //TODO quitar
    	
    	//Tipo de fecha:
    	//Puede ser 1,2 o 3 para buses, metro y FGC
    	BasicDBList or = new BasicDBList();	
    	BasicDBObject searchQueryTipoFechaBusMetro = new BasicDBObject();
    	searchQueryTipoFechaBusMetro.put("tipo_fecha", String.valueOf(dateType));
    	or.add(searchQueryTipoFechaBusMetro);
    	
    	BasicDBObject searchQueryTipoFechaFGC = new BasicDBObject();
    	searchQueryTipoFechaFGC.put("tipo_fecha", dateType);
    	or.add(searchQueryTipoFechaFGC);
    	
    	//o 4 para parkings bicing,etc pq no dependen del dia
    	BasicDBObject searchQueryTipoFechaOtros = new BasicDBObject();
    	searchQueryTipoFechaOtros.put("tipo_fecha", 4);
    	or.add(searchQueryTipoFechaOtros);
    	
    	searchQuery.put("$or", or);
		
		searchQuery.put("desde", new BasicDBObject("$lte", hora));
		searchQuery.put("hasta", new BasicDBObject("$gte", hora));	
		
		/*BasicDBList or = new BasicDBList();		
		
		BasicDBObject searchQueryHoraMetro = new BasicDBObject();
		searchQueryHoraMetro.put("desde", new BasicDBObject("$lte", hora));
		searchQueryHoraMetro.put("hasta", new BasicDBObject("$gte", hora));	
		or.add(searchQueryHoraMetro);
		
		BasicDBObject searchQueryHoraBUS = new BasicDBObject();
		searchQueryHoraBUS.put("arrival_time", new BasicDBObject("$gte", hora));	
		searchQueryHoraBUS.put("departure_time", new BasicDBObject("$lte", hora));			
		or.add(searchQueryHoraBUS);
		
		searchQuery.put("$or", or);*/
		
		searchQuery.put("lat",new BasicDBObject("$gte",Double.toString(lat - distance_q)).append("$lte",Double.toString(lat + distance_q)));
		searchQuery.put("lon",new BasicDBObject("$gte",Double.toString(lon - distance_q)).append("$lte",Double.toString(lon + distance_q)));
	
		
    	return searchQuery;
    }
    
    //https://www.outsystems.com/forums/discussion/33208/distance-google-maps/
    private double getDistancia(double latEvento, double lonEvento, double latTransporte, double lonTransporte){
    	  double R = 6371.0; // Radius of the earth in km
    	  double dLat = deg2rad(latEvento - latTransporte);  // deg2rad below
    	  double dLon = deg2rad(lonEvento - lonTransporte);
    	  double a =
    	    Math.sin(dLat/2) * Math.sin(dLat / 2) +
    	    Math.cos(deg2rad(latEvento)) * Math.cos(deg2rad(latTransporte)) *
    	    Math.sin(dLon / 2) * Math.sin(dLon / 2)
    	    ;
    	  double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    	  double d = (R * c) *1000; // Distance in m
    	  return Math.abs(d);    	
    }
    
    private double deg2rad(double deg) {
    	  return deg * (Math.PI/180);
    }
}
