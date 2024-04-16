import mysql.connector
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

table_create_query = """
CREATE TABLE products (
  id CHAR(11) PRIMARY KEY NOT NULL,
  title VARCHAR(255) NOT NULL,
  processed TINYINT NOT NULL DEFAULT 0,
  category varchar(255) NOT NULL
);

CREATE UNIQUE INDEX id_pk ON products (id);

CREATE TABLE reviews (
  id INT primary key not null AUTO_INCREMENT,
  product_id CHAR(11) NOT NULL,
  helpfulness float DEFAULT 0,
  rating FLOAT NOT NULL,
  summary VARCHAR(512) NULL DEFAULT NULL,
  processed BIT NOT NULL DEFAULT 0
);
ALTER TABLE reviews
ADD CONSTRAINT FK_product_reviews
FOREIGN KEY (product_id) REFERENCES products(id);
CREATE TABLE processed_review (
  id int AUTO_INCREMENT primary key not null,
  review_id CHAR(11) NOT NULL,
  metric_type VARCHAR(255) NULL DEFAULT NULL,
  metric blob DEFAULT NULL
);
ALTER TABLE processed_reviews
ADD CONSTRAINT FK_reviews_analysis
FOREIGN KEY (review_id) REFERENCES reviews(id);

CREATE UNIQUE INDEX review_id_metric_pk ON processed_reviews (review_id, metric_type);
"""

con = mysql.connector.connect(
    user="root", password="root", host="mysql-server", database="product_analysis"
)

curs = con.cursor(dictionary=True)
curs.execute("DROP TABLE IF EXISTS processed_review")
curs.execute("DROP TABLE IF EXISTS reviews")
curs.execute("DROP TABLE IF EXISTS products")

res = curs.execute(table_create_query)
con.close()
print("Tables created successfully.")
spark.stop()