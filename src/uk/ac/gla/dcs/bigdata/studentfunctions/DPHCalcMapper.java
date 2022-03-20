package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Arrays;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

public class DPHCalcMapper implements MapFunction<Query,DocumentRanking>{

    Broadcast<List<Query>> queryBroadcast;
    Broadcast<List<NewsArticle>> newsArticleListBroadcast;
    Broadcast<Long> totalDocsCountBroadcast;
	Broadcast<Double> averageDocumentLengthBroadcast;
	Broadcast<List<Tuple2<String, Short>>> termFrequenciesListBroadcast;
	LongAccumulator currDocumentLength;
    DoubleAccumulator avgScoreAcc;
    LongAccumulator termFrequencyInCorpus;
    LongAccumulator termFrequencyInDocument;
    Broadcast<List<NewsArticle>> originalNewsArticleListBroadcast;

    private transient TextPreProcessor processor;


    public DPHCalcMapper(Broadcast<List<Query>> queryBroadcast,
            Broadcast<List<NewsArticle>> newsArticleListBroadcast,
            Broadcast<Long> totalDocsCountBroadcast, Broadcast<Double> averageDocumentLengthBroadcast,
            Broadcast<List<Tuple2<String, Short>>> termFrequenciesListBroadcast,
            LongAccumulator currDocumentLength, DoubleAccumulator avgScoreAcc,LongAccumulator termFrequencyInCorpus,
            LongAccumulator termFrequencyInDocument, Broadcast<List<NewsArticle>> originalNewsArticleListBroadcast) {
        this.queryBroadcast = queryBroadcast;
        this.newsArticleListBroadcast = newsArticleListBroadcast;
        this.totalDocsCountBroadcast = totalDocsCountBroadcast;
        this.averageDocumentLengthBroadcast = averageDocumentLengthBroadcast;
        this.termFrequenciesListBroadcast = termFrequenciesListBroadcast;
        this.currDocumentLength = currDocumentLength;
        this.avgScoreAcc = avgScoreAcc;
        this.termFrequencyInCorpus = termFrequencyInCorpus;
        this.termFrequencyInDocument = termFrequencyInDocument;
        this.originalNewsArticleListBroadcast = originalNewsArticleListBroadcast;
    }

    @Override
    public DocumentRanking call(Query query) throws Exception {
        
        if (processor==null) processor = new TextPreProcessor();

        List<RankedResult> rankedResults = new ArrayList<RankedResult>();

        List<Tuple2<String, Short>> termFrequenciesList = termFrequenciesListBroadcast.value();

        List<NewsArticle> originalNewsArticleList = originalNewsArticleListBroadcast.value();
        newsArticleListBroadcast.value().forEach(newsArticle->{
            
            avgScoreAcc.setValue(0.0);
            query.getQueryTerms().forEach(term->{
                termFrequencyInDocument.setValue(0);
                double score = 0.0;
                List<String> concatList = new ArrayList<String>();
                concatList.add(newsArticle.getTitle());
                
                currDocumentLength.setValue(0);
                currDocumentLength.add(newsArticle.getTitle().length());

                newsArticle.getContents().forEach(content -> {
                    int paragraphs = 0;
                    if(content.getContent() != null && content.getSubtype() == "paragraph" && paragraphs < 5){
                        currDocumentLength.add(content.getContent().length());
                        concatList.add(content.getContent());
                        paragraphs += 1;
                    }
                });
                
                String joined = String.join("", concatList);
                List<String> joinedSplit = Arrays.asList(joined.split(" "));
                
                joinedSplit.forEach(str -> {
                    if(str.contains(term)){
                        termFrequencyInDocument.add(1);
                    }
                });

                termFrequenciesList.forEach(corpusTerm -> {
                    if(term.compareTo(corpusTerm._1) == 0) {
                        termFrequencyInCorpus.setValue(corpusTerm._2.shortValue());
                    }
                });
                
                score = DPHScorer.getDPHScore(
                    termFrequencyInDocument.value().shortValue(), 
                    (int)termFrequencyInCorpus.sum(),
                    (int)currDocumentLength.sum(), 
                    averageDocumentLengthBroadcast.value(), 
                    totalDocsCountBroadcast.value().longValue()
                );

                if (Double.isNaN(score))
					score = 0.0;
				
				avgScoreAcc.add(score);
            });
            int qsize = query.getQueryTerms().size();
            double finalScore = avgScoreAcc.sum()/qsize;

            rankedResults.add(
                    new RankedResult(
                            newsArticle.getId(), 
                            originalNewsArticleList.stream().filter(it -> it.getId().contentEquals(newsArticle.getId())).collect(Collectors.toList()).get(0),
                            finalScore));
        });
        
    Collections.sort(rankedResults);
    Collections.reverse(rankedResults);
    return new DocumentRanking(
            query,
            rankedResults
        );
    }

    /**
	 * Utility method that converts a List<String> to a string
	 * @param terms
	 * @return
	 */
	public String terms2String(List<String> terms) {
		StringBuilder builder = new StringBuilder();
		for (String term : terms) {
			builder.append(term);
			builder.append(" ");
		}
		return builder.toString();
	}
    
}
