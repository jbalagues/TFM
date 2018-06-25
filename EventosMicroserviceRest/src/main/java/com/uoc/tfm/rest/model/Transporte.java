package com.uoc.tfm.rest.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_EMPTY)
public class Transporte {

	private List<EventoBean> coordenadas = new ArrayList<EventoBean>();



}
