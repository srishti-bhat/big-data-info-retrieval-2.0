package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

/**
 * Testing map class that simply applies the text pre-processor on all text
 * @author Richard
 *
 */
public class TestTokenize implements MapFunction<NewsArticle,NewsArticle> {

	private static final long serialVersionUID = 4638169702466249304L;
	
	private transient TextPreProcessor processor;
	
	/**
	 * Called for each news article, applies the text pre-processor on all text in the article
	 */
	@Override
	public NewsArticle call(NewsArticle article) throws Exception {
		
		if (processor==null) processor = new TextPreProcessor();

		String title = terms2String(processor.process(article.getTitle()));
		article.setTitle(title);
		
		for (int i =0; i<article.getContents().size(); i++) {
			ContentItem content = article.getContents().get(i);
			if (content.getContent()!=null) {
				String processedContent = terms2String(processor.process(content.getContent()));
				content.setContent(processedContent);
			}
		}
		
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
