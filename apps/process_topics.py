import json
from sparknlp.base import DocumentAssembler
from pyspark.sql import functions as F
import pyspark.sql.types as T
import mysql.connector
from pyspark.sql import SparkSession
import nltk
from sparknlp.annotator import Tokenizer
from sparknlp.annotator import Normalizer
from sparknlp.annotator import LemmatizerModel
from nltk.corpus import stopwords
from sparknlp.annotator import StopWordsCleaner
from sparknlp.annotator import PerceptronModel
from sparknlp.annotator import Chunker
from sparknlp.base import Finisher
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF
from pyspark.ml.clustering import LDA

nltk.download("stopwords")

con = mysql.connector.connect(
    user="root", password="root", host="mysql-server", database="product_analysis"
)

curs = con.cursor(dictionary=True)
INSERT_QUERY = "INSERT ignore INTO processed_review (review_id, metric_type, metric) VALUES (%s,%s,%s)"

spark = (
    SparkSession.builder.appName("processor")
    .master("local[*]")
    .config("spark.driver.memory", "16G")
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3")
    .config("spark.jars.packages", "mysql-connector-java-8.0.13")
    .getOrCreate()
)

products = (
    spark.read.format("jdbc")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("url", "jdbc:mysql://mysql-server:3306/product_analysis")
    .option("dbtable", "products")
    .option("user", "root")
    .option("password", "root")
    .load()
)


documentAssembler = DocumentAssembler().setInputCol("summary").setOutputCol("document")


tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("tokenized")


# Normalize data


normalizer = (
    Normalizer()
    .setInputCols(["tokenized"])
    .setOutputCol("normalized")
    .setLowercase(True)
)


lemmatizer = (
    LemmatizerModel.pretrained().setInputCols(["normalized"]).setOutputCol("lemmatized")
)


eng_stopwords = stopwords.words("english")


stopwords_cleaner = (
    StopWordsCleaner()
    .setInputCols(["lemmatized"])
    .setOutputCol("unigrams")
    .setStopWords(eng_stopwords)
)


pos_tagger = (
    PerceptronModel.pretrained("pos_anc")
    .setInputCols(["document", "lemmatized"])
    .setOutputCol("pos")
)


allowed_tags = ["<JJ>+<NN>", "<NN>+<NN>"]
chunker = (
    Chunker()
    .setInputCols(["document", "pos"])
    .setOutputCol("ngrams")
    .setRegexParsers(allowed_tags)
)


finisher = Finisher().setInputCols(["ngrams", "ngrams"])


pipeline = Pipeline().setStages(
    [
        documentAssembler,
        tokenizer,
        normalizer,
        lemmatizer,
        stopwords_cleaner,
        pos_tagger,
        chunker,
        finisher,
    ]
)


def process_reviews(product_id):
    try:
        reviews = (
            spark.read.format("jdbc")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("url", "jdbc:mysql://mysql-server:3306/product_analysis")
            .option("numPartitions", 5)
            .option("query", f"select * from reviews where product_id = {product_id}")
            .option("user", "root")
            .option("password", "root")
            .load()
        )
        reviews.show()
    except Exception as e:
        print(e)

    try:
        text = reviews.select("summary", "id")

        processed_review = pipeline.fit(text).transform(text)
        processed_review.show()

        tfizer = CountVectorizer(inputCol="finished_ngrams", outputCol="tf_features")

        tf_model = tfizer.fit(processed_review)
        tf_result = tf_model.transform(processed_review)

        idfizer = IDF(inputCol="tf_features", outputCol="tf_idf_features")
        idf_model = idfizer.fit(tf_result)
        tfidf_result = idf_model.transform(tf_result)

        num_topics = 6
        max_iter = 10
        lda = LDA(k=num_topics, maxIter=max_iter, featuresCol="tf_idf_features")
        lda_model = lda.fit(tfidf_result)

        vocab = tf_model.vocabulary

        def get_words(token_list):
            return [vocab[token_id] for token_id in token_list]

        udf_to_words = F.udf(get_words, T.ArrayType(T.StringType()))

        num_top_words = 7
        topics = lda_model.describeTopics(num_top_words).withColumn(
            "topicWords", udf_to_words(F.col("termIndices"))
        )
        topics_list = topics.select("topicWords").collect()
        INSERT_QUERY = "INSERT ignore INTO agg_product_review (product_id, metric_type, metric) VALUES (%s,%s,%s)"
        consolidated_payload = []
        for t in topics_list:
            consolidated_payload.extend(t.topicWords)
        consolidated_payload = list(set(consolidated_payload))
        print(consolidated_payload)
        curs.execute(
            INSERT_QUERY, (product_id, "topic_analysis", json.dumps(consolidated_payload))
        )
        con.commit()
    except Exception as e:
        print(f"SOMETHING HAS GONE WRONG!!!!!!!!!!!!!!!!:{e}")



products.show()
for product_id in products.collect():
    product_id = product_id.id
    process_reviews(product_id)
