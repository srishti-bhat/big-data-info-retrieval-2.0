package uk.ac.gla.dcs.bigdata.providedfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

/**
 * Converts a Row containing a String Json news article into a NewsArticle object 
 * @author Richard
 *
 */
public class NewsFormaterMap implements MapFunction<Row,NewsArticle> {

	private static final long serialVersionUID = 4631168869449498097L;

	private transient ObjectMapper jsonMapper;
	private transient TextPreProcessor processor;

    LongAccumulator totalDocumentLengthInCorpusAcc;
	LongAccumulator totalDocsInCorpusAcc;
	
	public NewsFormaterMap(LongAccumulator totalDocumentLengthInCorpusAcc, LongAccumulator totalDocsInCorpusAcc) {
		this.totalDocumentLengthInCorpusAcc = totalDocumentLengthInCorpusAcc;
		this.totalDocsInCorpusAcc = totalDocsInCorpusAcc;
	}

	@Override
	public NewsArticle call(Row value) throws Exception {

		if (processor==null) processor = new TextPreProcessor();
		if (jsonMapper==null) jsonMapper = new ObjectMapper();
		
		NewsArticle article = jsonMapper.readValue(value.mkString(), NewsArticle.class);

		String title = terms2String(processor.process(article.getTitle()));
		article.setTitle(title);
		
		for (int i =0; i<article.getContents().size(); i++) {
			ContentItem content = article.getContents().get(i);
			if (content.getContent()!=null) {
				String processedContent = terms2String(processor.process(content.getContent()));
				content.setContent(processedContent);
				if(processedContent != null){
                    totalDocumentLengthInCorpusAcc.add(title.length() + processedContent.length());
                }
			}
		}
		totalDocsInCorpusAcc.add(1);
		
		return article;
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
