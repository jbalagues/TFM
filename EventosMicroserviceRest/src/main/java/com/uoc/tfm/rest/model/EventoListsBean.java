package com.uoc.tfm.rest.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_EMPTY)
public class EventoListsBean {

	private List<EventoBean> eventos = new ArrayList<EventoBean>();


	public List<EventoBean> getEventos() {
		return eventos;
	}


	public void setEventos(List<EventoBean> eventos) {
		this.eventos = eventos;
	}


	public void  addEvento(EventoBean evento){
		eventos.add(evento);
	}
	
}
