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
				.config("spark.mongodb.input.uri", "mongodb://vaisham92:marias@ds131041.mlab.com:31041/history.URLRepository")
				.getOrCreate();

		// Creating spark context object ( entry point for spark) 
		JavaSparkContext sparkContextObject = new JavaSparkContext(sparkSessionObject.sparkContext());
		JavaMongoRDD<Document> URLMappingRDD = MongoSpark.load(sparkContextObject);
		
		// Loading URL Repository collection
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("collection", "h05062017");
		readOverrides.put("readPreference.name", "secondaryPreferred");
		ReadConfig readConfig = ReadConfig.create(sparkContextObject).withOptions(readOverrides);
		JavaMongoRDD<Document> browsingHistoryRDD = MongoSpark.load(sparkContextObject,readConfig);
		List<Document> filteredRecords =  Arrays.asList(Document.parse("{ $project: {user_id : 1 , _id:0, 'chromeHistory.hostname':1}}"));
		JavaMongoRDD<Document> aggregatedBrowsingHistoryRDD = browsingHistoryRDD.withPipeline(filteredRecords);
		List<Document> recordsList = aggregatedBrowsingHistoryRDD.collect();


		JSONObject json;
		JSONArray historyArray;
		List<String> userId_URL_List = new ArrayList<String>();

		// Data transformation 
		for(Document doc : recordsList) {	
			//To fetch single attribute in record
			json =  new JSONObject(doc.toJson());
			historyArray = (JSONArray) json.get("chromeHistory");
			for (Object host : historyArray) {
				JSONObject chromeDataJSON = new JSONObject(host.toString());
				String hostName = (String) chromeDataJSON.get("hostname");
				try {
				String query = "{$match : {URLDomain: \""+hostName+"\"}}";
				List<Document> filteredURLRecords = Arrays.asList(Document.parse(query));
				JavaMongoRDD<Document> aggregatedURLRDD = URLMappingRDD.withPipeline(filteredURLRecords);
				List<Document> urlRecordsList = aggregatedURLRDD.collect();
				JSONObject json1 = new JSONObject(urlRecordsList.get(0).toJson());	
				Object ob1 = json1.get("URLId");			
				int id = json.get("user_id").toString().hashCode();
				userId_URL_List.add(id+","+ob1);
				} catch (Exception e) {
					System.out.println("Hostname::::" + hostName);
				}
			}
		}

		// Creating RDD
		JavaRDD<String> lines = sparkContextObject.parallelize(userId_URL_List);
		JavaPairRDD<String,Integer> counts = lines
				.mapToPair(line -> new Tuple2<>(line,1))
				.reduceByKey((a,b)->a+b);
		JavaRDD<String> output = counts.map(a -> a._1 + "," + a._2);
		deleteDirectory();
		output.saveAsTextFile("output");
		
		//user-based recommendation
		BasicConfigurator.configure();
		DataModel model = new FileDataModel(new File("output/part-00000"));
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		double threshold = -0.1;
		UserNeighborhood neighborhood = new ThresholdUserNeighborhood(threshold, similarity, model);
		UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);

		MongoDatabase mDB = connectToMongo();
		MongoCollection<Document> collection = mDB.getCollection("result");
		MongoCollection<Document> urlCollection = mDB.getCollection("URLRepository");
		
		JSONObject userBrowsingRecord = null;
		Bson updateFilter = null;
		Bson updateResult = null;
		Bson findUrlName = null;
		Bson updateOperationDocument = null;
		String userID = null;
		int totalUrlsToRecommend = 2;
		
		for(Document rec : recordsList) {	
			userBrowsingRecord = new JSONObject(rec.toJson());	
			userID = userBrowsingRecord.get("user_id").toString();

			List<RecommendedItem> recommendations = recommender.recommend(userID.hashCode(),totalUrlsToRecommend);

			for (RecommendedItem recommendation : recommendations) {
				System.out.println("Recommendation:::" + recommendation.getItemID() + recommendation.getValue());
//				updateFilter = new Document("user_id", userID);
//				findUrlName = new Document("URLId", recommendation.getItemID());
				//String urlname = urlCollection.find(findUrlName).first().getString("URLDomain");
//				updateResult = new Document("recommended_urls", urlname);
//				updateOperationDocument = new Document("$addToSet", updateResult);
				//collection.updateOne(updateFilter, updateOperationDocument);
			}
		}
		
		sparkContextObject.close();

	}
	
	private static MongoDatabase connectToMongo() {
		MongoClient mongoClient = new MongoClient("localhost" , 27017);
		MongoDatabase mDB = mongoClient.getDatabase("browsinghistory");
		return mDB;	
	}
	
	private static void deleteDirectory() {
		File directory = new File("output");
		if(directory.exists()){
			try{
				delete(directory);
			}catch(IOException e){
				e.printStackTrace();
				System.exit(0);
			}
		}
	}
	
	private static void delete(File file) throws IOException {
		if(file.isDirectory()){
			//directory is empty, then delete it
			if(file.list().length==0){
				file.delete();
			} else {
				String files[] = file.list();
				for (String temp : files) {
					//construct the file structure
					File fileDelete = new File(file, temp);
					//recursive delete
					delete(fileDelete);
				}
				//check the directory again, if empty then delete it
				if(file.list().length==0){
					file.delete();
				}
			}
		} else{
			//if file, then delete it
			file.delete();
		}
	}
}
