package com.browsingInsights.mongoConnector;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

/**
 * Created by maverick on 5/7/17.
 */
public class MongoConnector {
    private MongoClient mongoClient;

    // Function to connect to Mongo
    private MongoDatabase connectToDB(){
        MongoClientURI mongoClientURI = new MongoClientURI("mongodb://vaisham92:marias@ds131041.mlab.com:31041/history");
        this.mongoClient = new MongoClient(mongoClientURI);
        return mongoClient.getDatabase("history");
    }


    // Function to disconnect from Mongo
    private void disconnectDB(){
        this.mongoClient.close();
    }
}
