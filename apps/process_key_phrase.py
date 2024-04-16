from pyspark.sql import SparkSession
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.base import *
import pandas as pd
import json
import mysql.connector
from sparknlp.pretrained import PretrainedPipeline

# Import the required modules and classes
from sparknlp.base import DocumentAssembler, Pipeline, LightPipeline
from sparknlp.annotator import SentenceDetector, Tokenizer, YakeKeywordExtraction
import pyspark.sql.functions as F

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

# Step 1: Transforms raw texts to `document` annotation
document = DocumentAssembler().setInputCol("text").setOutputCol("document")

# Step 2: Sentence Detection
sentenceDetector = SentenceDetector().setInputCols("document").setOutputCol("sentence")

# Step 3: Tokenization
token = (
    Tokenizer()
    .setInputCols("sentence")
    .setOutputCol("token")
    .setContextChars(["(", ")", "?", "!", ".", ","])
)

# Step 4: Keyword Extraction
keywords = (
    YakeKeywordExtraction()
    .setThreshold(0.7)
    .setInputCols("token")
    .setOutputCol("keywords")
)
# Define the pipeline
yake_pipeline = Pipeline(stages=[document, sentenceDetector, token, keywords])

# Create an empty dataframe
empty_df = spark.createDataFrame([[""]]).toDF("text")

# Fit the dataframe to get the
yake_Model = yake_pipeline.fit(empty_df)
light_model = LightPipeline(yake_Model)

products = (
    spark.read.format("jdbc")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("url", "jdbc:mysql://mysql-server:3306/product_analysis")
    .option("dbtable", "products")
    .option("user", "root")
    .option("password", "root")
    .load()
)

products.show()
product_id = products.select("id").take(10)[-1].id

print("p_id: ", product_id)


def get_reviews(product_id):
    # try:
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
    for row in reviews.select("summary", "id").collect():
        annotations = light_model.fullAnnotate(row.summary)
        # keys_df = pd.DataFrame(
        #     [
        #         (k.result, k.begin, k.end, k.metadata["score"], k.metadata["sentence"])
        #         for k in annotations[0]["keywords"]
        #     ],
        #     columns=["keywords", "begin", "end", "score", "sentence"],
        # )
        # keys_df["score"] = keys_df["score"].astype(float)
        if len(annotations[0]["keywords"]) == 0:
            continue
        payload = {
            "metric_type": "key_phrases",
            "metric": {
                "phrases": [
                    {
                        "phrase": k.result,
                        "score": k.metadata["score"],
                        "sentence": k.metadata["sentence"],
                    }
                    for k in annotations[0]["keywords"]
                ]
            },
            "review_id": row.id,
        }
        curs.execute(
            INSERT_QUERY,
            (
                payload["review_id"],
                payload["metric_type"],
                json.dumps(payload["metric"]),
            ),
        )
        print(f"{INSERT_QUERY}, {payload['review_id']}")
        con.commit()
        # ordered by relevance
        # print(keys_df.sort_values(["sentence", "score"]).head(10))
        # results.append(list(zip(annotations['lemmas'],annotations['pos'])))
        # print(results)
    # except Exception as e:
    #     print("Error: ", e)


product_ids = products.select(["id", "title"]).collect()

for p_id in product_ids:
    get_reviews(p_id.id)

spark.stop()
