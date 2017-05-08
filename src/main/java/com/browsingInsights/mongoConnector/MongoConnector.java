package com.browsingInsights.mongoConnector;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

/**
 * Created by maverick on 5/7/17.
 */
public class MongoConnector {
    public MongoClient mongoClient;

    // Function to connect to Mongo
    public MongoDatabase connectToDB(){
        MongoClientURI mongoClientURI = new MongoClientURI("mongodb://vaisham92:marias@ds131041.mlab.com:31041/history");
        this.mongoClient = new MongoClient(mongoClientURI);
        return mongoClient.getDatabase("history");
    }

    // Function to disconnect from Mongo
    public void disconnectDB(){
        this.mongoClient.close();
    }
}
