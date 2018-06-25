package com.uoc.tfm.test;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public class TestMongoDB {
	final static Logger logger = Logger.getLogger(TestMongoDB.class);
	
	public TestMongoDB() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		 SparkSession spark = null;
		try {
			logger.debug("Inicio ejecucion");
			
			spark = SparkSession.builder().master("local").appName("MongoSparkConnectorETL")
					.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.enventosDiario")
				    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.enventosDiario")
					.getOrCreate();
			logger.debug("SparkSession creada");
			JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		    /*Start Example: Read data from MongoDB************************/
		    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
		    /*End Example**************************************************/

		    // Analyze data from MongoDB
		    logger.debug(rdd.count());
		    logger.debug(rdd.first().toJson());

		    jsc.close();
		    }catch (Exception e){
		    	logger.error(e);
		    } finally {
		    	spark.close();
		    }
	}

}
