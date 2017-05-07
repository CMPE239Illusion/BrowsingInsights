import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by maverick on 5/1/17.
 */

class GenerateRules 
{
    // Function to initiate Spark and compute association rules
    public ArrayList<JSONObject> generateRules(List<String> input){
        SparkConf sparkConf = new SparkConf().setAppName("AssociationRulesGenerator").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> data = sc.parallelize(input);

        JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(" ")));

        FPGrowth fpg = new FPGrowth().setMinSupport(0.5).setNumPartitions(10);
        FPGrowthModel<String> model = fpg.run(transactions);

        double minConfidence = 0.8;
        ArrayList<JSONObject> result = new ArrayList<>();
        List<AssociationRules.Rule<String>> rules = model.generateAssociationRules(minConfidence).toJavaRDD().collect();
        int max = 0;
        for(AssociationRules.Rule<String> rule : rules){
            if(rule.javaAntecedent().size() > max){
                max = rule.javaAntecedent().size();
            }
        }

        for (AssociationRules.Rule<String> rule : rules) {
            if(rule.javaAntecedent().size() < max) continue;
            JSONObject tempObj = new JSONObject();
            tempObj.put("LHS",rule.javaAntecedent());
            tempObj.put("RHS", rule.javaConsequent());
            tempObj.put("conf", rule.confidence());
            result.add(tempObj);
        }
        sc.stop();
        return result;
    }
}