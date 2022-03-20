package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;

import org.apache.spark.api.java.function.MapGroupsFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleTermMap;

public class TermCountSum implements MapGroupsFunction<String, NewsArticleTermMap, Tuple2<String,Short>> {

    @Override
    public Tuple2<String, Short> call(String key, Iterator<NewsArticleTermMap> newsArticleTermMapItr) throws Exception {
        int count = 0;
		while (newsArticleTermMapItr.hasNext()) {
			NewsArticleTermMap newsArticleTermMap = newsArticleTermMapItr.next();
			count += newsArticleTermMap.getCount();
		}
	    return new Tuple2<String,Short>(key,(short)count);
    }

    
}
