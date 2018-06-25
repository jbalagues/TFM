package com.uoc.tfm.beans;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class EventoGenericoBean {

	protected  String  idEvento= null;
	
	protected  String evento = null;
	
	protected  String idCentro = null;
	protected  String centro = null;
	protected  String direccion = null;
	protected  String barrio = null;
	protected  String distrito = null;
	protected  String ciudad = null;
	
	protected  String lat = null;
	protected  String lon = null;
    
	protected  String  beginDate = null;
	protected  String  endDate = null;
	protected  String  beginHour= "0000";
	protected  String  endHour= "2359";
	
	public EventoGenericoBean(){
		
	}
	public EventoGenericoBean(String ciudad, NodeList itemChildNodes, Logger logger) {
		this.ciudad= ciudad;
		for (int j = 0; j < itemChildNodes.getLength(); ++j) {		    		
    		if(itemChildNodes.item(j) instanceof Element){
    			Element elemento = (Element) itemChildNodes.item(j);
    			String nombre =  elemento.getTagName();
    			String valor = "";
    			if( elemento.hasChildNodes()){
    				if(elemento.getFirstChild().getNodeType() == 4){    			
	    				valor =  elemento.getFirstChild().getNodeValue();
	    				logger.debug("  --->" +  nombre + "=" +  valor + "-" + elemento.getFirstChild().getNodeType());
	    			}else{
	    				NodeList barriChildNodes =  elemento.getElementsByTagName("barri");
	    				Element elementoBarri = (Element) barriChildNodes.item(0);
	    				if(elementoBarri!=null){
	    					valor =  elementoBarri.getFirstChild().getNodeValue();
		    				logger.debug("  --->" +  nombre + "=" +  valor + "-");
	    				}
	    				
	    			}
    			}
    			
    			switch(nombre){    			
    				case "id": 
    					this.idEvento = valor;
    					break;
    				case "name": 
    					this.evento = valor;
	   					break;	
    					
    				case "equipment_id": 
    					this.idCentro= valor;
    					break;		
    				case "institutionname": 
    					this.centro = valor;
    					break;    					
    				case "address": 
    					this.direccion = valor;
    					break;
    				case "addresses": 
    					this.barrio = valor;
    					break;
    				case "district": 
    					this.distrito = valor;
    					break;	
    				
    				case "gmapx": 
    					this.lat = valor;
	   					break;
    				case "gmapy": 
    					this.lon = valor;
	   					break;
	   					
    				case "begindate": 
    					if(valor!= null && valor.length()>9){
    						String fecha=valor.substring(0, 10);
    						this.beginDate =fecha.substring(6, 10) + fecha.substring(3, 5) + fecha.substring(0, 2);
    					}    					
	   					break;
	   				case "enddate": 
	   					if(valor!= null && valor.length()>9){
    						String fecha=valor.substring(0, 10);
    						this.endDate =fecha.substring(6, 10) + fecha.substring(3, 5) + fecha.substring(0, 2);
    					}   
	   					break;
	   					
	   				case "proxhour": 
    					this.beginHour = valor.replace(":", "");
	   					break;	   	
	   			
    			}
    		}
    	}
	}
	public String getIdEvento() {
		return idEvento;
	}
	public void setIdEvento(String idEvento) {
		this.idEvento = idEvento;
	}
	public String getEvento() {
		return evento;
	}
	public void setEvento(String evento) {
		this.evento = evento;
	}
	public String getCentro() {
		return centro;
	}
	public void setCentro(String centro) {
		this.centro = centro;
	}
	public String getDireccion() {
		return direccion;
	}
	public void setDireccion(String direccion) {
		this.direccion = direccion;
	}
	public String getBarrio() {
		return barrio;
	}
	public void setBarrio(String barrio) {
		this.barrio = barrio;
	}
	public String getDistrito() {
		return distrito;
	}
	public void setDistrito(String distrito) {
		this.distrito = distrito;
	}
	public String getCiudad() {
		return ciudad;
	}
	public void setCiudad(String ciudad) {
		this.ciudad = ciudad;
	}
	public String getLat() {
		return lat;
	}
	public void setLat(String lat) {
		this.lat = lat;
	}
	public String getLon() {
		return lon;
	}
	public void setLon(String lon) {
		this.lon = lon;
	}
	public String getBeginDate() {
		return beginDate;
	}
	public void setBeginDate(String beginDate) {
		this.beginDate = beginDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public String getBeginHour() {
		return beginHour;
	}
	public void setBeginHour(String beginHour) {
		this.beginHour = beginHour;
	}
	public String getEndHour() {
		return endHour;
	}
	public void setEndHour(String endHour) {
		this.endHour = endHour;
	}
	public String getIdCentro() {
		return idCentro;
	}
	public void setIdCentro(String idCentro) {
		this.idCentro = idCentro;
	}

}
