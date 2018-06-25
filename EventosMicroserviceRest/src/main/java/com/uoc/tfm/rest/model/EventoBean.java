package com.uoc.tfm.rest.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_EMPTY)
public class EventoBean {

	private  String ciudad = null;
	
	private  String  idEvento= null;
	
	private  String evento = null;
	
	private  String idCentro = null;
	private  String centro = null;
	private  String direccion = null;
	private  String barrio = null;
	private  String distrito = null;
	
	
	private  String lat = null;
	private  String lon = null;
    
	private  String  beginDate = null;
	private  String  endDate = null;
	private  String  beginHour= "0000";
	private  String  endHour= "2359";
	
	
	public EventoBean() {
		// TODO Auto-generated constructor stub
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


	public String getIdCentro() {
		return idCentro;
	}


	public void setIdCentro(String idCentro) {
		this.idCentro = idCentro;
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

}
