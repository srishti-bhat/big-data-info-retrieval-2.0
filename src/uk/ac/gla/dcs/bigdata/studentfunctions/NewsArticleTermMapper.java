package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleTermMap;

/**
	 * Transformation of NewsArticle to NewsArticleTermMap Dataset
*/
public class NewsArticleTermMapper implements FlatMapFunction<NewsArticle, NewsArticleTermMap>{

    Broadcast<List<Query>> queryBroadcast;
    LongAccumulator termCountInDocument;


    public NewsArticleTermMapper(Broadcast<List<Query>> queryBroadcast, LongAccumulator termCountInDocument) {
        this.queryBroadcast = queryBroadcast;
        this.termCountInDocument = termCountInDocument;
    }


    @Override
    public Iterator<NewsArticleTermMap> call(NewsArticle newsArticle) throws Exception {
        List<Query> queryBroadcastList = queryBroadcast.value();
        List<String> queryTermsFlattened = new ArrayList<String>();
        List<NewsArticleTermMap> newsArticleTermMapList = new ArrayList<NewsArticleTermMap>();

        queryBroadcastList.forEach(query -> {
            query.getQueryTerms().forEach(queryTerm -> {
                queryTermsFlattened.add(queryTerm); //query terms flattened to simplify iteration of terms
            });
        });

        queryTermsFlattened.forEach(term->{
            termCountInDocument.setValue(0); //resetting the accumulator value to 0
            List<String> concatList = new ArrayList<String>(); //list created to calculate term count

                concatList.add(newsArticle.getTitle()); //adding title to the list
                newsArticle.getContents().forEach(content -> {
                    int paragraphs = 0;
                    if(content.getContent() != null && content.getSubtype() == "paragraph" && paragraphs < 5){
                        concatList.add(content.getContent()); //adding paragraphs to the list (upto 5)
                        paragraphs += 1;
                    }
                });

                String joined = String.join("", concatList); //Combining List of strings into one String 
                List<String> joinedSplit = Arrays.asList(joined.split(" ")); //Splitting the contents of the String to form individual tokenised words
                
                joinedSplit.forEach(str -> {
                    if(str.contains(term)){
                        termCountInDocument.add(1); //counting the occurence of the term
                    }
                });

            newsArticleTermMapList.add(
                new NewsArticleTermMap(
                    term,
                    newsArticle,
                    termCountInDocument.value().shortValue()
                )
            );
        });
  
        return newsArticleTermMapList.iterator();
    }
    
}
