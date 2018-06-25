angular
  .module("eventosApp", ['ngMap','ngMaterial', 'ngMessages', 'material.svgAssetsCache','ui.bootstrap', 'ps.inputTime'])
  .controller("EventosAppController", 
		  		["$scope", "$http", "$filter", "NgMap", "$mdSidenav",
		  		function($scope, $http, $filter, NgMap, $mdSidenav, $mdDateLocaleProvider){
	//Inicializacion de parametros
	//-------------------------------	  
	 $scope.isOpen = false;
	 $scope.fecha = new Date();
	 $scope.time = $scope.fecha.getTime();
	 $scope.tabselected=0;
	 $scope.ciudad="BCN";
	 
	 //Radio del evento por defecto
     $scope.radio_min = 0;
     $scope.radio_max = 2000;
     $scope.radio_porcentaje = 20;
     
	 //Calendario: Ratio de fechas es que esta disponible:
     //Un aña atras desde la fecha de hoy a 1 año despues
     $scope.minDate = new Date(
		     $scope.fecha.getFullYear() - 1,
		     $scope.fecha.getMonth(),
		     $scope.fecha.getDate());
     
     $scope.maxDate = new Date(
		     $scope.fecha.getFullYear() + 1,
		     $scope.fecha.getMonth(),
		     $scope.fecha.getDate());		
	
	//Inicializacion del objeto de busqueda
	$scope.search = {
		ciudad: $scope.ciudad,
		distrito : "",
		agenda: "",
		fecha:new Date(),
		hora: new Date(),
		radio_percent: 20,
		radio: 400 //metros
    };
	
	//Inicializacion del objeto de busqueda de medios de transporte
	$scope.transporte = {
		ciudad: $scope.ciudad,
		lat : "",
		lon: "",
		fecha:new Date(),
		hora: new Date(),
		radio: 400 //metros
	};
	
	 //Inicializo el evento inicial
	 $scope.eventoSeleccionado = {
		evento : "Seleccione un evento mediante el buscador.",
		puntos: 0.0
	 };
	 

    $scope.first=true;
    
    //Coordenadas por defecto
    $scope.barcelona_latitud = 41.3874615;
    $scope.barcelona_longitud = 2.1673908;
    $scope.ratio_event = 400;//(400 /6378.1) * 6378100;
				
	//Inicializacion de los combos de tipos se evento y distritos
  	$http.get("soporte/agenda.json","charset=utf-8")
	    .then(function (data) {
	    	$scope.agendaList = data;
	    	console.log("agenda cargada")
	    }, function(res){
	    	console.log(res.status)
	});

	$http.get("soporte/distrito.json","charset=utf-8")
    	.then(function (data) {
    		$scope.distritoList = data;
    	}, function(res){
    		console.log(res.status)
	});
 
	$scope.eventosOrder='evento';
	
	 $scope.click = function() {
		 $scope.map.setCenter(barcelona);
	 };
		
	//Funcion que convierte el % que retorna el control deslizable a metros y que tambien actualiza el radio del circulo
	$scope.calculateRadiusSearch = function() {
	    $scope.search.radio= $scope.radio_min + (($scope.search.radio_percent * ($scope.radio_max - $scope.radio_min)) / 100);	
	    $scope.ratio_event =  $scope.search.radio;
	}; 	

     
     //Busqueda de los eventos que coinciden con los criterios de busqueda
     $scope.buscar = function () {
    	 var url= "/EventosRest/service/search/events";
      
                
         $http.post(url, angular.toJson($scope.search), {'Content-Type': 'application/json'})
         	.then(function (data) {
		    	//$scope.coordenadasList = data;		    	
		    	$scope.eventosList = angular.fromJson(data);
		    	console.log("eventos cargados")
		    	$scope.tabselected=1; 
		    	$scope.eventosOrder='evento';
		    	
		    }, function(res){ 
		    	console.log(res.status)
		    	 $scope.tabselected=0; 
		});
         
     };     
     
     //Buscar los medios de transporte cercanos al evento seleccioado
     $scope.irEvento = function (idEvento) {
    	 $scope.first=false;
    	 
    	 //Actualizo el mapa con la ubicacion del evento seleccionado
    	 for (var i = 0; i <= $scope.eventosList.data.eventos.length; i++) {
    	     evento = $scope.eventosList.data.eventos[i];
    	    
    	     if( evento.idEvento == idEvento){
    	    	 $scope.eventoSeleccionado = evento
    	    	 $scope.eventoSeleccionado.puntos=0.0;  	 
    	    	 $scope.barcelona_latitud = evento.lat;
    	    	 $scope.barcelona_longitud =evento.lon;
    	    	 i = $scope.eventosList.data.eventos.length;
    	     }
    	 }
    	 
    	 //Seteo los valorea en el bean de busqueda de trasportes
    	 $scope.transporte.ciudad=$scope.ciudad;
    	 $scope.transporte.lat=evento.lat;
    	 $scope.transporte.lon=evento.lon; 
    	 $scope.transporte.radio=$scope.search.radio; 
    	 $scope.transporte.fecha=$scope.search.fecha; 
    	 $scope.transporte.hora=$scope.search.hora;     	 
    	    	 
    	 $scope.radio_porcentaje = $scope.search.radio_percent;
    	 $scope.calculateRadiusSearch();
    	 
    	 //Inicializo la url y se procede a llamar al webservice
    	 var url= "/TransporteRest/service/search/transports";
    	 $http.post(url, angular.toJson($scope.transporte), {'Content-Type': 'application/json'})
	      	.then(function (data) {
			    	$scope.coordenadasList = angular.fromJson(data);		    	
			    	console.log("coordendas transporte cargadas");
			    	
			    	$scope.eventoSeleccionado.puntos=0.0;
			    	var length = $scope.coordenadasList.data.transportes.length;
			    	for (i = 0; i < length; i++) {
			    		//Sumo los puntos de transporte de cada resultado
			    		$scope.eventoSeleccionado.puntos = $scope.eventoSeleccionado.puntos + parseFloat($scope.coordenadasList.data.transportes[i].puntos);
			    		$scope.coordenadasList.data.transportes[i].puntos = $scope.coordenadasList.data.transportes[i].puntos.toFixed(1);
			    		//En funcion d ela agencia y el medio de transporte se asocia a cada punto de transporte un icono
			    		if($scope.coordenadasList.data.transportes[i].agencia == "TMB"){
		    				if($scope.coordenadasList.data.transportes[i].tipo_ruta == "1"){
		    					$scope.coordenadasList.data.transportes[i].icono="img/metro-logo.png";				    		  
		    				}else{
		    					$scope.coordenadasList.data.transportes[i].icono="img/bus-logo.png";
		    				}		    		  
		    			}else if($scope.coordenadasList.data.transportes[i].agencia == "FGC"){
		    				$scope.coordenadasList.data.transportes[i].icono="img/fgc-logo.png";
		    			}else if($scope.coordenadasList.data.transportes[i].agencia == "BICING"){
		    				$scope.coordenadasList.data.transportes[i].icono="img/bicing.png";
		    			}else if($scope.coordenadasList.data.transportes[i].agencia == "PARKING_BICI"){
		    				$scope.coordenadasList.data.transportes[i].icono="img/pkBici";
		    			}else if($scope.coordenadasList.data.transportes[i].agencia == "TAXI"){
		    				$scope.coordenadasList.data.transportes[i].icono="img/taxi.png";
		    			}else if($scope.coordenadasList.data.transportes[i].agencia == "PARKING"){
		    				$scope.coordenadasList.data.transportes[i].icono="img/aparcamiento.png";
		    			}			    	  
			    	};  
			    	$scope.eventoSeleccionado.puntos = $scope.eventoSeleccionado.puntos.toFixed(0);
			    	//$scope.eventosList = angular.fromJson(data);
			    }, function(res){ 
			    	console.log(res.status)
			    	 $scope.tabselected=1; 
			    });
    	 $scope.tabselected=2;
     }
     $scope.icono  = function () {

		  return "img/metro-logo.png";
     }  
		  
	 $scope.forceUnknownOption = function() {
	     $scope.data.agenda = null;
	     $scope.data.distrito = null;
	 };
	 
	 $scope.toggleLeft = buildToggler('left');

	    function buildToggler(componentId) {
	      return function() {
	        $mdSidenav(componentId).toggle();
	      };
	 }
	    
	$scope.borrarHora= function() {
	  
	    $scope.search.hora=null;
	    alert('Hola');
	 };
	    
	$scope.ratio_event=0;
	//Presentacion    
    $scope.calculateRadius = function() {
	    var ratioEarsKM=6378.1;
	    var ratioMeter= $scope.radio_min + (($scope.radio_porcentaje * ($scope.radio_max - $scope.radio_min)) / 100);		 
        $scope.ratio_event = ratioMeter;//(ratioMeter /6378.1) * 6378100
	}; 
	
	 $scope.changeRadius = function() {
		 $scope.calculateRadius();
		 $scope.transporte.radio=$scope.ratio_event; 		 
		 //Inicializo la url y se procede a llamar al webservice
    	 var url= "/TransporteRest/service/search/transports";
    	 $http.post(url, angular.toJson($scope.transporte), {'Content-Type': 'application/json'})
	      	.then(function (data) {
			    	$scope.coordenadasList = angular.fromJson(data);		    	
			    	console.log("coordendas transporte cargadas");
			    	
			    	$scope.eventoSeleccionado.puntos=0.0;
			    	var length = $scope.coordenadasList.data.transportes.length;
			    	for (i = 0; i < length; i++) {
			    		//Sumo los puntos de transporte de cada resultado
			    		$scope.eventoSeleccionado.puntos = $scope.eventoSeleccionado.puntos + parseFloat($scope.coordenadasList.data.transportes[i].puntos);
		    			
			    		//En funcion d ela agencia y el medio de transporte se asocia a cada punto de transporte un icono
			    		if($scope.coordenadasList.data.transportes[i].agencia == "TMB"){
		    				if($scope.coordenadasList.data.transportes[i].tipo_ruta == "1"){
		    					$scope.coordenadasList.data.transportes[i].icono="img/metro-logo.png";				    		  
		    				}else{
		    					$scope.coordenadasList.data.transportes[i].icono="img/bus-logo.png";
		    				}		    		  
		    			}else if($scope.coordenadasList.data.transportes[i].agencia == "FGC"){
		    				$scope.coordenadasList.data.transportes[i].icono="img/fgc-logo.png";
		    			}else if($scope.coordenadasList.data.transportes[i].agencia == "BICING"){
		    				$scope.coordenadasList.data.transportes[i].icono="img/bicing.png";
		    			}else if($scope.coordenadasList.data.transportes[i].agencia == "PARKING_BICI"){
		    				$scope.coordenadasList.data.transportes[i].icono="img/pkBici.png";
		    			}else if($scope.coordenadasList.data.transportes[i].agencia == "TAXI"){
		    				$scope.coordenadasList.data.transportes[i].icono="img/taxi.png";
		    			}else if($scope.coordenadasList.data.transportes[i].agencia == "PARKING"){
		    				$scope.coordenadasList.data.transportes[i].icono="img/aparcamiento.png";
		    			}			    	  
			    	};  
			    	
			    	//$scope.eventosList = angular.fromJson(data);
			    }, function(res){ 
			    	console.log(res.status)
			    	 $scope.tabselected=1; 
			    });
    	 $scope.tabselected=2;
    	 
	 }
	
	
		
		/*NgMap.getMap().then(function(map) {
			$scope.showCustomMarker= function(evt) {
	            map.customMarkers.evento.setVisible(true);
	            map.customMarkers.evento.setPosition(this.getPosition());
	         };
	         $scope.closeCustomMarker= function(evt) {
	            this.style.display = 'none';
	         };
        });*/
		


  }]).config(function($mdDateLocaleProvider) {
	  $mdDateLocaleProvider.formatDate = function(date) {
	       return moment(date).format('DD-MM-YYYY');
	    };
	    // Example of a Spanish localization.
	    $mdDateLocaleProvider.months = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 
	                                    'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'];
	    $mdDateLocaleProvider.shortMonths = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 
	                                    'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'];
	    $mdDateLocaleProvider.days = ['Domingo', 'Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sábado'];
	    $mdDateLocaleProvider.shortDays = ['Do', 'Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sá'];
	    // Can change week display to start on Monday.
	    $mdDateLocaleProvider.firstDayOfWeek = 1;
	    // Optional.
	    //$mdDateLocaleProvider.dates = [1, 2, 3, 4, 5, 6, 7,8,9,10,11,12,13,14,15,16,17,18,19,
	    //                               20,21,22,23,24,25,26,27,28,29,30,31];
	    // In addition to date display, date components also need localized messages
	    // for aria-labels for screen-reader users.
	    $mdDateLocaleProvider.weekNumberFormatter = function(weekNumber) {
	      return 'Semana ' + weekNumber;
	    };
	    $mdDateLocaleProvider.msgCalendar = 'Calendario';
	    $mdDateLocaleProvider.msgOpenCalendar = 'Abrir calendario';
	});;