package com.browsingInsights.recommendation;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;

import com.browsingInsights.mongoConnector.MongoConnector;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;


import scala.Tuple2;

public class URLRecommendation {
	public static void getRecommendations(MongoDatabase mDB) throws InterruptedException, IOException, TasteException
	{
		// Creating spark configuration 
		SparkSession sparkSessionObject = SparkSession.builder()
				.master("local[1]")
				.appName("URLRecommendationComponent")
				.config("spark.mongodb.input.uri", "mongodb://vaisham92:marias@ds131041.mlab.com:31041/history.URLRepository")
				.getOrCreate();

		// Creating spark context object ( entry point for spark) 
		JavaSparkContext sparkContextObject = new JavaSparkContext(sparkSessionObject.sparkContext());
		JavaMongoRDD<Document> URLMappingRDD = MongoSpark.load(sparkContextObject);

		//Get yesterday's collection name to get the user browsing records
		String inputCollectionName = getYesterdayCollectionName(0);
		// Loading URL Repository collection
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("collection", inputCollectionName);
		readOverrides.put("readPreference.name", "secondaryPreferred");
		ReadConfig readConfig = ReadConfig.create(sparkContextObject).withOptions(readOverrides);
		JavaMongoRDD<Document> browsingHistoryRDD = MongoSpark.load(sparkContextObject,readConfig);
		List<Document> filteredRecords =  Arrays.asList(Document.parse("{ $project: {user_id : 1 , _id:0, 'chromeHistory.hostname':1}}"));
		JavaMongoRDD<Document> aggregatedBrowsingHistoryRDD = browsingHistoryRDD.withPipeline(filteredRecords);
		List<Document> recordsList = aggregatedBrowsingHistoryRDD.collect();

		MongoCollection<Document> urlCollection = mDB.getCollection("URLRepository");
		
		JSONObject json;
		JSONArray historyArray;
		List<String> userId_URL_List = new ArrayList<String>();
		String hostName = null;

		// Data transformation 
		for(Document doc : recordsList) {	
			//To fetch single attribute in record
			json =  new JSONObject(doc.toJson());
			historyArray = (JSONArray) json.get("chromeHistory");
			for (Object host : historyArray) {
				try {
					JSONObject chromeDataJSON = new JSONObject(host.toString());
					hostName = (String) chromeDataJSON.get("hostname");
					
					Bson urlId = new Document("URLDomain", hostName);
					int url_id = urlCollection.find(urlId).first().getInteger("URLId");
					
//					String query = "{$match : {URLDomain: \""+hostName+"\"}}";
//					List<Document> filteredURLRecords = Arrays.asList(Document.parse(query));
//					JavaMongoRDD<Document> aggregatedURLRDD = URLMappingRDD.withPipeline(filteredURLRecords);
//					List<Document> urlRecordsList = aggregatedURLRDD.collect();
//					JSONObject json1 = new JSONObject(urlRecordsList.get(0).toJson());	
//					Object ob1 = json1.get("URLId");			
					int id = json.get("user_id").toString().hashCode();
					userId_URL_List.add(id+","+url_id);
				} catch (Exception e) {
					System.out.println("Hostname::::" + hostName);
				}
			}
		}

		// Creating RDD - Map Reduce
		JavaRDD<String> lines = sparkContextObject.parallelize(userId_URL_List);
		JavaPairRDD<String,Integer> counts = lines
				.mapToPair(line -> new Tuple2<>(line,1))
				.reduceByKey((a,b)->a+b);
		JavaRDD<String> output = counts.map(a -> a._1 + "," + a._2);
		deleteDirectory();
		output.saveAsTextFile("IntermediateResult");

		//Get yesterday's collection name to store the recommended URL
		String outputCollectionName = getYesterdayCollectionName(1);

		JSONObject userBrowsingRecord = null;
		Bson updateFilter = null;
		Bson updateResult = null;
		Bson findUrlName = null;
		Bson updateOperationDocument = null;
		String userID = null;
		int totalUrlsToRecommend = 10;
		String urlname = null;
		int users = 4;

		//user-based recommendation - Apache Mahout
		BasicConfigurator.configure();
		DataModel model = new FileDataModel(new File("IntermediateResult/part-00000"));
		UserSimilarity similarity = new UncenteredCosineSimilarity(model);
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(users, similarity, model);
		UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);

		//connecting to mongo database to store the recommendation results
		MongoCollection<Document> collection = mDB.getCollection(outputCollectionName);
		//MongoCollection<Document> urlCollection = mDB.getCollection("URLRepository");

		for(Document rec : recordsList) {	
			urlname = null;
			userBrowsingRecord = new JSONObject(rec.toJson());	
			userID = userBrowsingRecord.get("user_id").toString();
			List<RecommendedItem> recommendations = recommender.recommend(userID.hashCode(),totalUrlsToRecommend);

				for (RecommendedItem recommendation : recommendations) {
					try {
					updateFilter = new Document("user_id", userID);
					findUrlName = new Document("URLId", recommendation.getItemID());
					urlname = urlCollection.find(findUrlName).first().getString("URLDomain");
					updateResult = new Document("recommended_urls", urlname);
					updateOperationDocument = new Document("$addToSet", updateResult);
					collection.updateOne(updateFilter, updateOperationDocument);
					} catch(Exception e) {
						System.out.println("URLId" + recommendation.getItemID());
					}
				}
		}

		sparkContextObject.close();

	}

	//Method to delete the generated output folder
	private static void deleteDirectory() {
		File directory = new File("IntermediateResult");
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

	//Method to get the collection name
	private static String getYesterdayCollectionName(int i){
		StringBuilder collectionName = new StringBuilder();
		if(i==0) collectionName.append("h");
		else collectionName.append("r");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		DateFormat dF = new SimpleDateFormat("MM/dd/yyyy");
		collectionName.append(dF.format(cal.getTime()).replace("/",""));
		return collectionName.toString();
	}
}