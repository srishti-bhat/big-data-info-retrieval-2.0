package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleTermMap;

/**
	 * Extract Term from the NewsArticleTermMap class
*/
public class TermKeyFunction implements MapFunction<NewsArticleTermMap,String> {

    @Override
    public String call(NewsArticleTermMap newsArticleTermMap) throws Exception {
        return newsArticleTermMap.getTerm();
    }
    
}
