package com.uoc.tfm.beans;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class EventoMensualBean extends EventoGenericoBean {
	Logger logger;
	private List<String> tipus =new ArrayList<String>();
	
	public EventoMensualBean(String city, NodeList itemChildNodes, Logger logger) {
		super();
		try{
			this.logger = logger;
			this.ciudad= city;
			for (int j = 0; j < itemChildNodes.getLength(); ++j) {		    		
	    		if(itemChildNodes.item(j) instanceof Element){
	    			Element elemento = (Element) itemChildNodes.item(j);
	    			String nombre =  elemento.getTagName();
	    			String valor = "";
	    			if( elemento.hasChildNodes()){
	    				if("lloc_simple".equals(nombre)){
	    					NodeList lloc_simpleList = elemento.getChildNodes();
	    					Node currentLloc = null;
	    				    for (int i = 0; i < lloc_simpleList.getLength(); i++) {
	    				    	valor = "";
	    				    	currentLloc = lloc_simpleList.item(i);
	    				    	if (currentLloc.getNodeType() == Node.ELEMENT_NODE) {
		    				        Element elementLloc = (Element) currentLloc;
		    				        nombre =  "lloc_simple_" + elementLloc.getTagName();
		    				        
		    				        if("adreca_simple".equals(elementLloc.getTagName())){
		    				        	NodeList adrecaList = elementLloc.getChildNodes();
		    	    					Node currentAdreca = null;
		    	    					valor = "";
		    	    					for (int a = 0; a < adrecaList.getLength(); a++) {
		    	    						valor = "";
		    	    						currentAdreca = adrecaList.item(a);
		    	    				    	if (currentAdreca.getNodeType() == Node.ELEMENT_NODE) {
		    		    				        Element elementAdreça = (Element) currentAdreca;
		    		    				        nombre =  "lloc_adreça_" + elementAdreça.getTagName();
		    		    				        if(elementAdreça.getFirstChild() != null){
		    		    				        	 valor = elementAdreça.getFirstChild().getNodeValue();
			    	    				        	setValores(nombre, valor);
			    	 	    				        logger.debug("  ------------->" +  nombre + "=" +  valor );
		    		    				        }
		    		    				       
		    	    				    	}
		    	    					}
		    				        	
		    				        }else{
		    				        	valor = elementLloc.getFirstChild().getNodeValue();
		    				        	setValores(nombre, valor);
		 	    				        //logger.debug("  ---------->" +  nombre + "=" +  valor );
		    				        }
		    				       
	    				    	}
	    				    }
	    				}else if("data".equals(nombre)){
	    					NodeList dataList = elemento.getChildNodes();
	    					Node dataItem = null;
	    					
	    				    for (int i = 0; i < dataList.getLength(); i++) {
	    				    	valor = "";
	    				    	dataItem = dataList.item(i);
	    				    	if (dataItem.getNodeType() == Node.ELEMENT_NODE) {
		    				        Element elementData = (Element) dataItem;
		    				        nombre =  "data_" + elementData.getTagName();
		    				        if(elementData.getFirstChild() != null && elementData.getFirstChild().getNodeValue()!=null){
		    				        	valor = elementData.getFirstChild().getNodeValue();
		      				        }
		    				        
	    				        	setValores(nombre, valor);
	 	    				        //logger.debug("  ---------->" +  nombre + "=" +  valor );	    				        
	    				    	}
	    				    }
	    				}else if("classificacions".equals(nombre)){
	    					NodeList classifList = elemento.getChildNodes();
	    					Node classifItem = null;
	    					
	    				    for (int i = 0; i < classifList.getLength(); i++) {
	    				    	valor = "";
	    				    	classifItem = classifList.item(i);
	    				    	if (classifItem.getNodeType() == Node.ELEMENT_NODE) {
		    				        Element elementData = (Element) classifItem;
		    				        nombre =  "classificacions_" + elementData.getTagName();
		    				        valor = elementData.getAttribute("codi");
		    				        
		    				        
	    				        	setValores(nombre, valor);
	 	    				        //logger.debug("  ---------->" +  nombre + "=" +  valor );	    				        
	    				    	}
	    				    }
	    				}else{
	    					valor =  elemento.getFirstChild().getNodeValue();    				
	    					setValores(nombre, valor);
	    				}    			  				
	    			}   		    			
	    		}
	    	}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private void setValores(String nombre, String valor){
		logger.debug("  ------>" +  nombre + "=" +  valor );
		switch(nombre){    			
			case "id": 
				this.idEvento = valor;
				break;
			case "nom": 
				this.evento = valor;
				break;
			case "lloc_simple_id": 
				this.idCentro = valor;
				break;						
			case "lloc_simple_nom": 
				this.centro = valor;
				break;    					
			case "lloc_adreça_carrer": 
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
					
			case "data_data_inici": 
				this.beginDate = valor;
					break;
			case "data_data_fi": 
				this.endDate= valor;
				break;
					
			case "data_hora_inici": 
				this.beginHour = valor;
					break;	
					
			case "classificacions_nivell": 
				this.tipus.add(valor) ;
					break;
			
		}
		
	}

}
