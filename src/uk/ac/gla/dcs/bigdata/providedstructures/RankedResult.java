package uk.ac.gla.dcs.bigdata.providedstructures;

import java.io.Serializable;

public class RankedResult implements Serializable, Comparable<RankedResult> {

	private static final long serialVersionUID = -2905684103776472843L;
	
	long docid;
	NewsArticle article;
	double score;
	
	public RankedResult() {}
	
	public RankedResult(long docid, NewsArticle article, double score) {
		super();
		this.docid = docid;
		this.article = article;
		this.score = score;
	}

	public long getDocid() {
		return docid;
	}

	public void setDocid(long docid) {
		this.docid = docid;
	}

	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public int compareTo(RankedResult o) {
		return new Double(score).compareTo(o.score);
	}
	
	
	
}
