package com.uoc.tfm.rest.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_EMPTY)
public class TransporteListBean {

	private List<TransporteBean> transportes = new ArrayList<TransporteBean>();

	public List<TransporteBean> getTransportes() {
		return transportes;
	}

	public void setTransportes(List<TransporteBean> transportes) {
		this.transportes = transportes;
	}



}
