package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
	 * Term mapped with a news article with it's frequency
*/
public class NewsArticleTermMap implements Serializable {

    private static final long serialVersionUID = 1565312130567946299L;

    String term;
    NewsArticle newsArticle; 
    short count; //count of the term in the news article

    public NewsArticleTermMap(String term, NewsArticle newsArticle, short count) {
        this.term = term;
        this.newsArticle = newsArticle;
        this.count = count;
    }
    
    public NewsArticleTermMap() {
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public NewsArticle getNewsArticle() {
        return newsArticle;
    }

    public void setNewsArticle(NewsArticle newsArticle) {
        this.newsArticle = newsArticle;
    }

    public short getCount() {
        return count;
    }

    public void setCount(short count) {
        this.count = count;
    }

    

}
