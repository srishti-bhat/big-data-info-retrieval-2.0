package uk.ac.gla.dcs.bigdata.apps;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentfunctions.DPHCalcMapper;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleTermMapper;
import uk.ac.gla.dcs.bigdata.studentfunctions.TermCountSum;
import uk.ac.gla.dcs.bigdata.studentfunctions.TermKeyFunction;
import uk.ac.gla.dcs.bigdata.studentfunctions.TestTokenize;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleTermMap;


/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef==null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
			sparkMasterDef = "local[2]"; // default is local mode with two executors
		}
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("BIGDATA_QUERIES");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("BIGDATA_NEWS");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out!=null) resultsDIR = out;
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(new File(resultsDIR).getAbsolutePath());
			}
		}
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath()+"/SPARK.DONE")));
			writer.write(String.valueOf(System.currentTimeMillis()));
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		newsjson = newsjson.repartition(24);

		LongAccumulator totalDocumentLengthInCorpusAcc = spark.sparkContext().longAccumulator();
		LongAccumulator totalDocsInCorpusAcc = spark.sparkContext().longAccumulator();
		LongAccumulator termCountInDocument = spark.sparkContext().longAccumulator();
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(totalDocumentLengthInCorpusAcc, totalDocsInCorpusAcc), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		
		// try a collect as list
		List<Query> queryList = queries.collectAsList();
		List<NewsArticle> newsList = news.collectAsList();

		Broadcast<List<Query>> queryBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryList);
		Broadcast<List<NewsArticle>> newsArticleBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(newsList);

		Dataset<NewsArticleTermMap> newsArticleTermMap = news.flatMap(new NewsArticleTermMapper(queryBroadcast, termCountInDocument), Encoders.bean(NewsArticleTermMap.class));	
		List<NewsArticleTermMap> newsArticleTermMapList = newsArticleTermMap.collectAsList();

		double averageDocumentLengthInCorpus = totalDocumentLengthInCorpusAcc.value() / totalDocsInCorpusAcc.value();

		KeyValueGroupedDataset<String, NewsArticleTermMap> termArticleGrouped = newsArticleTermMap.groupByKey(new TermKeyFunction(), Encoders.STRING());
		Encoder<Tuple2<String,Short>> termEncoder = Encoders.tuple(Encoders.STRING(), Encoders.SHORT());
		Dataset<Tuple2<String,Short>> termFrequencies = termArticleGrouped.mapGroups(new TermCountSum(), termEncoder);
		List<Tuple2<String, Short>> termFrequenciesList = termFrequencies.collectAsList();

		Broadcast<Long> totalDocsCountBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpusAcc.value());
		Broadcast<Double> averageDocumentLengthBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLengthInCorpus);
		Broadcast<List<Tuple2<String, Short>>> termFrequenciesListBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termFrequenciesList);
		Broadcast<List<NewsArticleTermMap>> newsArticleTermMapListtBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(newsArticleTermMapList);

		LongAccumulator currDocumentLength = spark.sparkContext().longAccumulator();
		DoubleAccumulator avgScoreAcc = spark.sparkContext().doubleAccumulator();
		LongAccumulator termFrequencyInCorpus = spark.sparkContext().longAccumulator();
		LongAccumulator termFrequencyInDocument = spark.sparkContext().longAccumulator();

		Dataset<DocumentRanking> documentRanking = queries.map(
			new DPHCalcMapper(
				queryBroadcast, 
				newsArticleBroadcast,
				totalDocsCountBroadcast, 
				averageDocumentLengthBroadcast, 
				termFrequenciesListBroadcast,
				currDocumentLength,
				avgScoreAcc,
				termFrequencyInCorpus,
				termFrequencyInDocument
				), 
			Encoders.bean(DocumentRanking.class));
		List<DocumentRanking> documentRankingList = documentRanking.collectAsList();

		Dataset<NewsArticle> newsTokenized = news.map(new TestTokenize(), Encoders.bean(NewsArticle.class));
		
		// collect some articles
		List<NewsArticle> first10 = news.takeAsList(10);
		
		// create a dummy document ranking manually so we can return something
		List<RankedResult> results = new ArrayList<RankedResult>(10);
		for (NewsArticle article : first10) {
			RankedResult result = new RankedResult(article.getId(), article, 1.0);
			results.add(result);
			
		}
		DocumentRanking ranking = new DocumentRanking(queryList.get(0), results);
		
		// convert to list
		List<DocumentRanking> rankingList = new ArrayList<DocumentRanking>(1);
		rankingList.add(ranking);
		
		return rankingList;

	}
	
	
}
