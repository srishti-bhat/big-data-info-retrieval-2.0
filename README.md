# Batch Based Text Search and Filtering Pipeline

The pipeline takes in a large set of text documents and a set of user defined queries, then for each query, text documents are ranked by relevance for that query. The top 10 documents for each query are returned as output. Each document and query is processed to remove stopwords and stemming is applied. Documents are scored using the DPH ranking model.

## Spark Topology

<figure align="center">
  <img width="900" 
  src="https://i.imgur.com/yDqC5sq.png"
  alt="Tokenisation of News Articles.">
  <figcaption>Tokenisation of News Articles</figcaption>
</figure>

<figure align="center">
  <img width="900" 
  src="https://i.imgur.com/PPdADsb.png"
  alt="Calculation of Total Frequency of each term in the entire dataset.">
  <figcaption>Calculation of Total Frequency of each term in the entire dataset</figcaption>
</figure>

<figure align="center">
  <img width="900" 
  src="https://i.imgur.com/pU9ak0R.png"
  alt="Ranking of Document against each query.">
  <figcaption>Ranking of Document against each query</figcaption>
</figure>


## Directory Structure

```
.
├── bin
├── data
│   ├── README.md
│   ├── TREC_Washington_Post_collection.v3.example.json		
│   └── queries.list						
├── resources
├── results						
│   └── 1646504628047								
│       ├── result.finance.documentranking
│       ├── result.james_bond.documentranking
│       ├── result.on_Facebook_IPO.documentranking
│       └── SPARK.DONE
├── src
│   └── uk
│       └── ac
│           └── gla
│               └── dcs
│                   └── bigdata
│                       ├── apps
│                       │   └── AssessedExercise.java	
│                       ├── providedfunctions
│                       │   ├── NewsFormaterMap.java
│                       │   └── QueryFormaterMap.java
│                       ├── providedstructures
│                       │   ├── ContentItem.java
│                       │   ├── DocumentRanking.java
│                       │   ├── NewsArticle.java
│                       │   ├── Query.java
│                       │   └── RankedResult.java
│                       ├── providedutilities
│                       │   ├── DPHScorer.java
│                       │   ├── TextDistanceCalculator.java
│                       │   └── TextPreProcessor.java
│                       ├── studentfunctions
│                       │   ├── DPHCalcMapper.java
│                       │   ├── NewsArticleTermMapper.java
│                       │   ├── RedundancyCheck.java
│                       │   ├── TermCountSum.java
│                       │   ├── TermKeyFunction.java
│                       │   └── TestTokenize.java
│                       │
│                       ├── studentstructures
│                       │   └── NewsArticleTermMap.java
│                       │
│                       └── util
│                           └── RealizationEngineClient.java
├── target 
├── README.md
└── pom.xml

```


