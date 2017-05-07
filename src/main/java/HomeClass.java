import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by maverick on 5/1/17.
 */

public class HomeClass {

    private static MongoClient mongoClient;
    private static HashMap<String, String> websiteCategoryMap;
    private static MongoCollection<Document> websiteCategoryCollection;
    private static MongoCollection<Document> previousDayHistoryCollection;
    private static MongoCollection<Document> previousDayResultCollection;
    private static List<Document> resultsRulesAndCategoryList;

    public static void main(String[] args){
        websiteCategoryMap = new HashMap<>();
        try{

            // Get Users list from MongoDb
            MongoDatabase mDB = connectToDB();

            // Retrieving Collection Name
            String cNameHistory = getYesterdayCollectionName(0);
            String cNameResult = getYesterdayCollectionName(1);
            previousDayHistoryCollection = mDB.getCollection(cNameHistory);
            previousDayResultCollection = mDB.getCollection(cNameResult);
            websiteCategoryCollection = mDB.getCollection("URLRepository");

            MongoCursor<Document> cursor = previousDayHistoryCollection.find().iterator();
            resultsRulesAndCategoryList = new ArrayList<>();

            while (cursor.hasNext()){
                JSONArray categoryTimeArray = new JSONArray();
                JSONObject currentUserRecord = new JSONObject(cursor.next().toJson());
                String userId = currentUserRecord.getString("user_id");
                JSONArray userChromeHistoryArray = currentUserRecord.getJSONArray("chromeHistory");
                ArrayList<JSONObject> rulesArray =  getRulesAndCategoryAnalysis(userChromeHistoryArray, categoryTimeArray);
                resultsRulesAndCategoryList.add(new Document().parse(new JSONObject().put("user_id", userId).put("rules", rulesArray).put("categories", categoryTimeArray).toString()));
            }
            for(Document d : resultsRulesAndCategoryList){
                System.out.println(d.toJson());
            }
        }
        catch(Exception ex) {
            if(ex.getClass().getName().equals("UnknownHostException")){
                System.out.println("Error in connecting to MongoDb");
            }
            else{
                System.out.println(ex.getClass().getName());
            }
        }
        finally {
            // Save rules & url category count in MongoDb
            previousDayResultCollection.insertMany(resultsRulesAndCategoryList);
            disconnectDB();
        }
    }

    // Function to connect to Mongo
    private static MongoDatabase connectToDB(){
        MongoClientURI mongoClientURI = new MongoClientURI("mongodb://vaisham92:marias@ds131041.mlab.com:31041/history");
        mongoClient = new MongoClient(mongoClientURI);
        return mongoClient.getDatabase("history");
    }

    // Function to generate yesterday CollectionName
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

    // Function to generate rules and category wise time
    private static ArrayList<JSONObject> getRulesAndCategoryAnalysis(JSONArray userChromeHistoryArray, JSONArray categoryTimeArray){

        // Break data into time slots and time spent
        HashMap<Integer, JSONObject> userChromeHistoryMap = breakData(userChromeHistoryArray);

        // Generate unique URL list
        HashSet<String> urlSet = new HashSet<>();
        for(int i : userChromeHistoryMap.keySet()) {
            urlSet.addAll(userChromeHistoryMap.get(i).keySet());
        }

        // Retrieve category of the url
        WebCategorizer wc = new WebCategorizer();
        wc.getWebCategories(urlSet, websiteCategoryMap, websiteCategoryCollection);

        // Generate time spent on each category in a time slot
        HashMap<Integer, JSONObject> chromeHistoryUrlTimeMap = new HashMap<>();
        computeCategoryTime(userChromeHistoryMap, websiteCategoryMap, chromeHistoryUrlTimeMap);

        for(int i : chromeHistoryUrlTimeMap.keySet()){
            JSONObject tempJSON = new JSONObject();
            tempJSON.put("slot", i);
            tempJSON.put("slot_categories", chromeHistoryUrlTimeMap.get(i));
            categoryTimeArray.put(tempJSON);
        }

        // Generate transactional data from userChromeHistoryMap
        List<String> transactionList = new ArrayList<>();
        buildTransactions(userChromeHistoryMap,transactionList);


        // Generate rules
        GenerateRules gr = new GenerateRules();
        return gr.generateRules(transactionList);
        //return new ArrayList<>();
    }

    // Function to break data set into HashMap
    private static HashMap<Integer, JSONObject> breakData(JSONArray userChromeHistoryArray){
        try{
            HashMap<Integer, JSONObject> userChromeHistoryMap = new HashMap<>();
            for(Object chromeHistoryUrlRecord : userChromeHistoryArray){
                addToSlot(userChromeHistoryMap, (JSONObject)chromeHistoryUrlRecord);
            }
            return userChromeHistoryMap;
        }
        catch(Exception ex) {
            System.out.println(ex.getClass().getName());
            return new HashMap<>();
        }
    }

    // Function to generate slots for url
    private static void addToSlot(HashMap<Integer, JSONObject> userChromeHistoryMap, JSONObject chromeHistoryUrlRecord){
        Timestamp startTimestamp = new Timestamp(chromeHistoryUrlRecord.getLong("T1"));
        Timestamp endTimestamp = new Timestamp(chromeHistoryUrlRecord.getLong("T2"));
        int key = startTimestamp.getHours();
        String url = chromeHistoryUrlRecord.getString("hostname");
        Calendar cal = Calendar.getInstance();
        Calendar cal1 = Calendar.getInstance();
        cal.setTimeInMillis(startTimestamp.getTime());
        cal1.setTimeInMillis(endTimestamp.getTime());
        Timestamp tempTimestamp;
        while(startTimestamp.getHours() != endTimestamp.getHours()){
            Calendar cal2 = Calendar.getInstance();
            cal2.setTimeInMillis(startTimestamp.getTime());
            cal2.add(Calendar.MINUTE, 59-startTimestamp.getMinutes());
            cal2.add(Calendar.SECOND, 59-startTimestamp.getSeconds());
            tempTimestamp = new Timestamp(cal2.getTime().getTime());
            key = startTimestamp.getHours();
            if(userChromeHistoryMap.containsKey(key)){
                JSONObject tempJSON = userChromeHistoryMap.get(key);
                if(tempJSON.has(url)){
                    tempJSON.put(url, (Long)tempJSON.get(url) + (tempTimestamp.getTime()-startTimestamp.getTime())/1000);
                }
                else{
                    tempJSON.put(url, (tempTimestamp.getTime()-startTimestamp.getTime())/1000);
                }
            }
            else{
                JSONObject tempJSON = new JSONObject();
                tempJSON.put(url, (tempTimestamp.getTime()-startTimestamp.getTime())/1000 );
                userChromeHistoryMap.put(key, tempJSON);
            }
            cal2.add(Calendar.SECOND, 1);
            startTimestamp = new Timestamp(cal2.getTime().getTime());
        }
        if(startTimestamp!=endTimestamp){
            if(userChromeHistoryMap.containsKey(key)){
                JSONObject tempJSON = userChromeHistoryMap.get(key);
                if(tempJSON.has(url)){
                    tempJSON.put(url, (Long)tempJSON.get(url) + (endTimestamp.getTime()-startTimestamp.getTime())/1000);
                }
                else{
                    tempJSON.put(url, (endTimestamp.getTime()-startTimestamp.getTime())/1000);
                }
            }
            else{
                JSONObject tempJSON = new JSONObject();
                tempJSON.put(url, (endTimestamp.getTime()-startTimestamp.getTime())/1000 );
                userChromeHistoryMap.put(key, tempJSON);
            }
        }
    }

    // Function to compute time spent on each category
    private static void computeCategoryTime( HashMap<Integer, JSONObject> userChromeHistoryMap, HashMap<String, String> websiteCategoryMap, HashMap<Integer, JSONObject> chromeHistoryUrlTimeMap){
        for(int i : userChromeHistoryMap.keySet()){
            JSONObject urlData = userChromeHistoryMap.get(i);
            for (Object url : urlData.keySet()) {
                JSONObject tempJSON = new JSONObject();
                String currentUrl = (String) url;
                String urlCategory = websiteCategoryMap.get(currentUrl);
                if(chromeHistoryUrlTimeMap.containsKey(i)){
                    tempJSON = chromeHistoryUrlTimeMap.get(i);
                    if(tempJSON.has(urlCategory)){
                        tempJSON.put(urlCategory, tempJSON.getLong(urlCategory) + urlData.getLong(currentUrl));
                    }
                    else {
                        tempJSON.put(urlCategory, urlData.getLong(currentUrl) );
                    }
                }
                else {
                    tempJSON.put(urlCategory, urlData.getLong(currentUrl) );
                    chromeHistoryUrlTimeMap.put(i, tempJSON);
                }
            }
        }
    }

    // Function to build transactional data from browsing history
    private static void buildTransactions(HashMap<Integer, JSONObject> userChromeHistoryMap, List<String> transactionList){
        for(int i : userChromeHistoryMap.keySet()){
            JSONObject tempJSON = userChromeHistoryMap.get(i);
            StringBuilder tempTransaction = new StringBuilder();
            for(String key : tempJSON.keySet()){
                if(tempJSON.getLong(key) > 30) {
                    tempTransaction.append(key+" ");
                }
            }
            if(tempTransaction.toString().length() != 0) transactionList.add(tempTransaction.toString());
        }
    }

    // Function to disconnect from Mongo
    private static void disconnectDB(){
        mongoClient.close();
    }

}
