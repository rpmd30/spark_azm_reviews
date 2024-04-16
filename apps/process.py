from pyspark.sql import SparkSession
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.base import *

spark = (
    SparkSession.builder.appName("processor")
    .master("local[*]")
    .config("spark.driver.memory", "16G")
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3")
    .config("spark.jars.packages", "mysql-connector-java-8.0.13")
    .getOrCreate()
)

from sparknlp.pretrained import PretrainedPipeline

pipeline = PretrainedPipeline("explain_document_ml")

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
        results = []
        for row in reviews.select("summary").collect():
            print(row)
            annotations = pipeline.annotate(row.summary)
            results.append(list(zip(annotations['lemmas'],annotations['pos'])))
        print(results)
    except Exception as e:
        print("Error: ", e)

product_ids = products.select(["id",'title']).collect()

for p_id in product_ids:
    get_reviews(p_id.id)

spark.stop()