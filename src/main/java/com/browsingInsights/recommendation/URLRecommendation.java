package com.browsingInsights.recommendation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;


import scala.Tuple2;

public class URLRecommendation {
	public static void main( String[] args ) throws InterruptedException, IOException, TasteException
	{
		// Creating spark configuration 
		SparkSession sparkSessionObject = SparkSession.builder()
				.master("local")
				.appName("URLRecommendationComponent")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/browsinghistory.h05052017")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/browsinghistory.genericrules")
				.getOrCreate();

		// Creating spark context object ( entry point for spark) 
		JavaSparkContext sparkContextObject = new JavaSparkContext(sparkSessionObject.sparkContext());
		JavaMongoRDD<Document> browsingHistoryRDD = MongoSpark.load(sparkContextObject);
		List<Document> filteredRecords =  Arrays.asList(Document.parse("{ $project: {user_id : 1 , _id:0, 'history.hostname':1}}"));
		JavaMongoRDD<Document> aggregatedBrowsingHistoryRDD = browsingHistoryRDD.withPipeline(filteredRecords);
		List<Document> recordsList = aggregatedBrowsingHistoryRDD.collect();

		// Loading URL Repository collection
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("collection", "URLRepository");
		readOverrides.put("readPreference.name", "secondaryPreferred");
		ReadConfig readConfig = ReadConfig.create(sparkContextObject).withOptions(readOverrides);
		JavaMongoRDD<Document> URLMappingRDD = MongoSpark.load(sparkContextObject,readConfig);

		JSONObject json;
		JSONArray historyArray;
		List<String> userId_URL_List = new ArrayList<String>();

		// Data transformation 
		for(Document doc : recordsList) {	
			//To fetch single attribute in record
			json =  new JSONObject(doc.toJson());
			historyArray = (JSONArray) json.get("history");
			List<String> hostnameList = new ArrayList<String>();
			for (Object host : historyArray) {
				JSONObject chromeDataJSON = new JSONObject(host.toString());
				String hostName = (String) chromeDataJSON.get("hostname");
				hostnameList.add(hostName);

				String query = "{$match : {URLDomainName : \""+hostName+"\"}}";
				List<Document> filteredURLRecords = Arrays.asList(Document.parse(query));
				JavaMongoRDD<Document> aggregatedURLRDD = URLMappingRDD.withPipeline(filteredURLRecords);
				List<Document> urlRecordsList = aggregatedURLRDD.collect();
				JSONObject json1 = new JSONObject(urlRecordsList.get(0).toJson());	
				Object ob1 = json1.get("URLId");			
				int id = json.get("user_id").toString().hashCode();
				userId_URL_List.add(id+","+ob1);
			}
		}

		// Creating RDD
		JavaRDD<String> lines = sparkContextObject.parallelize(userId_URL_List);
		JavaPairRDD<String,Integer> counts = lines
				.mapToPair(line -> new Tuple2<>(line,1))
				.reduceByKey((a,b)->a+b);
		JavaRDD<String> output = counts.map(a -> a._1 + "," + a._2);
		output.saveAsTextFile("output");

	}
}
