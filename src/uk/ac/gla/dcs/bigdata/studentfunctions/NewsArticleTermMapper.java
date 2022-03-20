package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleTermMap;

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
                queryTermsFlattened.add(queryTerm);
            });
        });

        queryTermsFlattened.forEach(term->{
            termCountInDocument.setValue(0);
            List<String> concatList = new ArrayList<String>();
                concatList.add(newsArticle.getTitle());
                newsArticle.getContents().forEach(content -> {
                    if(content.getContent() != null){
                        concatList.add(content.getContent());
                    }else if(content.getSubtype() == "image"){
                        concatList.add(content.getBlurb());
                    }
                });

                
                String joined = String.join("", concatList);
                List<String> joinedSplit = Arrays.asList(joined.split(" "));
                
                joinedSplit.forEach(str -> {
                    if(str.contains(term)){
                        termCountInDocument.add(1);
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
