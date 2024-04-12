from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("processor")
    .master("local[*]") \
    .config("spark.driver.memory", "16G") \
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

products.show()
product_id = products.select("id").take(10)[-1].id

print("p_id: ", product_id)

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

# high_helpful_reviews = reviews.filter(reviews.helpfulness == 1).show()
helpful_reviews = reviews.join(products, reviews.product_id == products.id).show()

# from sparknlp.pretrained import PretrainedPipeline

# explain_document_pipeline = PretrainedPipeline("explain_document_ml")
# annotations = explain_document_pipeline.annotate("We are very happy about SparkNLP")
# print(annotations)
# print(type(annotations))
