from pyspark.sql import SparkSession; import re; import traceback;
from pyspark.sql.functions import udf, col, length, concat_ws 
from pyspark.sql.types import StringType, IntegerType, DoubleType, ArrayType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer, VectorAssembler, NGram
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("CPU_IMP").config("spark.driver.memory", "8g").getOrCreate() 
data_path = "data/train.csv"

"""
- hp tuning (tree icin n_estimators, max_depth, min_samples_split, min_samples_leaf, max_features, criterion)
"""

def count_punctuation(text):
    if text is None: return 0
    return len(re.findall(r'[?!]', text))

def avg_word_length(text):
    if text is None: return 0.0
    words = text.split()
    if not words: return 0.0
    return sum(len(word) for word in words) / len(words)

def remove_html_tags(text):
    if text is None: return None
    return re.sub(re.compile('<.*?>'), '', text)

def clean_tags(tags):
    if tags is None: return []
    return tags.replace('<', ' ').replace('>', ' ').strip().split()

count_punct_udf = udf(count_punctuation, IntegerType())
avg_word_len_udf = udf(avg_word_length, DoubleType())
remove_html_udf = udf(remove_html_tags, StringType())
clean_tags_udf = udf(clean_tags, ArrayType()) 

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
    df_clean = df.na.drop(subset=["Title", "Body", "Y"]).withColumn("CleanBody", remove_html_udf(col("Body")))\
        .withColumn("text", concat_ws(" ", col("Title"), col("CleanBody")))
    
    label_indexer = StringIndexer(inputCol="Y", outputCol="label", handleInvalid="skip")
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=20000)
    idf = IDF(inputCol="raw_features", outputCol="text_features")
    
    df_featured = df_clean.withColumn("title_len", length(col("Title"))) \
        .withColumn("body_len", length(col("CleanBody"))) \
        .withColumn("punct_count", count_punct_udf(col("text"))) \
        .withColumn("avg_word_len", avg_word_len_udf(col("text")))

    feature_assembler = VectorAssembler(
        inputCols=["text_features", "title_len", "body_len", "punct_count", "avg_word_len"],
        outputCol="features"
    )

    (train_data, test_data) = df_featured.randomSplit([0.8, 0.2], seed=42)
    train_data.cache(); test_data.cache()
    
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
    lr_pipeline = Pipeline(stages=[label_indexer, tokenizer, stopwords_remover, hashing_tf, idf, feature_assembler, lr])
    lr_model = lr_pipeline.fit(train_data)
    lr_predictions = lr_model.transform(test_data)
    
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    accuracy = evaluator.setMetricName("accuracy").evaluate(lr_predictions)
    f1_score = evaluator.setMetricName("f1").evaluate(lr_predictions)
    
    print("\nLogistic Regression Evaluation")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"F1 Score: {f1_score:.4f}")
    print("Confusion Matrix:")
    lr_predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()


    rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=100)
    rf_pipeline = Pipeline(stages=[label_indexer, tokenizer, stopwords_remover, hashing_tf, idf, feature_assembler, rf])
    
    rf_model = rf_pipeline.fit(train_data)
    rf_predictions = rf_model.transform(test_data)
    accuracy = evaluator.setMetricName("accuracy").evaluate(rf_predictions)
    f1_score = evaluator.setMetricName("f1").evaluate(rf_predictions)

    print("\nRandom Forest Evaluation")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"F1 Score: {f1_score:.4f}")
    print("Confusion Matrix:")
    rf_predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

    spark.stop()

except Exception as e:
    print(f"{e}")
    traceback.print_exc()
    spark.stop()