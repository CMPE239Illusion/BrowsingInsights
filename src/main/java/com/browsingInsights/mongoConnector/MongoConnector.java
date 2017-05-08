package com.browsingInsights.mongoConnector;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

/**
 * Created by maverick on 5/7/17.
 */
public class MongoConnector {
    private static MongoClient mongoClient=null;
    
    // Function to connect to Mongo
    public static MongoDatabase connectToDB(){
    	if(mongoClient==null){
    		MongoClientURI mongoClientURI = new MongoClientURI("mongodb://vaisham92:marias@ds131041.mlab.com:31041/history");
    		mongoClient = new MongoClient(mongoClientURI);
    	}
        return mongoClient.getDatabase("history");
    }
    // Function to disconnect from Mongo
    public static void disconnectDB(){
        mongoClient.close();
    }
}
