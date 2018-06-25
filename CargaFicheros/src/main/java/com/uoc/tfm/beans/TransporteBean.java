package com.uoc.tfm.beans;

import java.io.Serializable;

public class TransporteBean implements Serializable {

	private String id;
	
	private String barrio;
	private String distrito;
	private String ciudad;
    
	private String agency = null; //TMB, FGC, TAXI stop, Bicing
	private String datetype = null;//laborable, sabado o festivo
	private String tipo_ruta = null; //bus, metro
   
	private String cod_linea= null;//L!,L2, S55
	private String nom_linea= null;//Les Corts / Av. Tibidabo  

	private String cod_parada= null; //L1,L2, S55,
	private String nom_parada = null; //112, Sant Antoni Maria Claret - Lepant

    private String lat = null;
	private String lon = null;
    
    private String desde= null;
    private String hasta= null;
    
	private String frec= null;
	private String puntuacion= null;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getCiudad() {
		return ciudad;
	}
	public void setCiudad(String ciudad) {
		this.ciudad = ciudad;
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
	public String getAgency() {
		return agency;
	}
	public void setAgency(String agency) {
		this.agency = agency;
	}
	public String getDatetype() {
		return datetype;
	}
	public void setDatetype(String datetype) {
		this.datetype = datetype;
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
	public String getNom_parada() {
		return nom_parada;
	}
	public void setNom_parada(String nom_parada) {
		this.nom_parada = nom_parada;
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
	public String getFrec() {
		return frec;
	}
	public void setFrec(String frec) {
		this.frec = frec;
	}
	public String getPuntuacion() {
		return puntuacion;
	}
	public void setPuntuacion(String puntuacion) {
		this.puntuacion = puntuacion;
	}
	
	
}
