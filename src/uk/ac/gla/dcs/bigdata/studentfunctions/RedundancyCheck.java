package uk.ac.gla.dcs.bigdata.studentfunctions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

public class RedundancyCheck implements MapFunction<DocumentRanking,DocumentRanking> {

    @Override
    public DocumentRanking call(DocumentRanking documentRanking) throws Exception {
        List<RankedResult> inputResultsSet = documentRanking.getResults();

        Collections.sort(inputResultsSet);
        Collections.reverse(inputResultsSet);

        List<RankedResult> outputResultsSet = new ArrayList<RankedResult>();

        for(int i = 0; i < documentRanking.getResults().size(); i++){
            for(int j = i + 1; j < documentRanking.getResults().size(); j++){
                if(TextDistanceCalculator.similarity(
                    inputResultsSet.get(i).getArticle().getTitle(), 
                    inputResultsSet.get(j).getArticle().getTitle()
                    ) < 0.5){
                    inputResultsSet.remove(inputResultsSet.get(j)); 
                }
            }
        }
        for(int k =0; k < 10; k++){
            outputResultsSet.add(inputResultsSet.get(k));
        }

        return new DocumentRanking(documentRanking.getQuery(), outputResultsSet);
    }

}
