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
        
        for (int i = 0; i < inputResultsSet.size(); i++) {

            int flag = 0;
            RankedResult inputDoc= inputResultsSet.get(i);

            for (int j = 0; j < outputResultsSet.size(); j++) {
                if (TextDistanceCalculator.similarity(
                    inputDoc.getArticle().getTitle(),outputResultsSet.get(j).getArticle().getTitle())
                     < 0.5) {
                    flag = 1;
                    break;
                }
            }
            if (flag == 0) {
                outputResultsSet.add(inputDoc);
            }
            if (outputResultsSet.size() == 10) {
                break;
            } 
        }; 
        return new DocumentRanking(documentRanking.getQuery(), outputResultsSet);
    }

}
