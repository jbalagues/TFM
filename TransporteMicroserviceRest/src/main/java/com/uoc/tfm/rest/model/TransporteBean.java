package com.uoc.tfm.rest.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_EMPTY)
public class TransporteBean {

	private String ciudad = null;
	
	private String  agencia = null;
	private String  tipo_fecha = null;
	private String  tipo_ruta = null;
	private String  cod_linea = null;
	private String  nom_linea = null;
	private String  cod_parada = null;
	private String  parada = null;
	private String  desde = null;
	private String  hasta = null;
	private String  freq = null;
	private Double  puntos = null;
	private String  distancia = null;
	
	private String lat = null;
	private String lon = null;
	public String getCiudad() {
		return ciudad;
	}
	public void setCiudad(String ciudad) {
		this.ciudad = ciudad;
	}
	public String getAgencia() {
		return agencia;
	}
	public void setAgencia(String agencia) {
		this.agencia = agencia;
	}
	public String getTipo_fecha() {
		return tipo_fecha;
	}
	public void setTipo_fecha(String tipo_fecha) {
		this.tipo_fecha = tipo_fecha;
	}
	public String getTipo_ruta() {
		return tipo_ruta;
	}
	public void setTipo_ruta(String tipo_ruta) {
		this.tipo_ruta = tipo_ruta;
	}
	public String getCod_linea() {
		return cod_linea;
	}
	public void setCod_linea(String cod_linea) {
		this.cod_linea = cod_linea;
	}
	public String getNom_linea() {
		return nom_linea;
	}
	public void setNom_linea(String nom_linea) {
		this.nom_linea = nom_linea;
	}
	public String getCod_parada() {
		return cod_parada;
	}
	public void setCod_parada(String cod_parada) {
		this.cod_parada = cod_parada;
	}
	public String getParada() {
		return parada;
	}
	public void setParada(String parada) {
		this.parada = parada;
	}
	public String getDesde() {
		return desde;
	}
	public void setDesde(String desde) {
		this.desde = desde;
	}
	public String getHasta() {
		return hasta;
	}
	public void setHasta(String hasta) {
		this.hasta = hasta;
	}
	public String getFreq() {
		return freq;
	}
	public void setFreq(String freq) {
		this.freq = freq;
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
	public String getDistancia() {
		return distancia;
	}
	public void setDistancia(String distancia) {
		this.distancia = distancia;
	}
	public Double getPuntos() {
		return puntos;
	}
	public void setPuntos(Double puntos) {
		this.puntos = puntos;
	}
    
	
	

}
