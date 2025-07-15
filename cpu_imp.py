from pyspark.sql import SparkSession; import re;
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("CPU_IMP").getOrCreate() 
# .config("spark.driver.memory", "4g") ?
data_path = "data/train.csv"

try:
    df = spark.read.format("csv") \
                    .option("header", "true") \
                    .option("quote", "\"") \
                    .option("escape", "\"") \
                    .option("multiLine", "true") \
                    .option("inferSchema", "true") \
                    .load(data_path)
    df.printSchema()
    df.groupBy("Y").count().show()
    initial_count = df.count()
    df_clean = df.na.drop(subset=["Title", "Body", "Y"])
    cleaned_count = df_clean.count()
    
    def remove_html_tags(text):
            if text is None:
                return None
            clean = re.compile('<.*?>')
            return re.sub(clean, '', text)

    remove_html_udf = udf(remove_html_tags, StringType())
    df_clean = df_clean.withColumn("CleanBody", remove_html_udf(col("Body")))
    
except Exception as e:
    print("?")
    spark.stop()