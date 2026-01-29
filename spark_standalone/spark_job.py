from pyspark.sql import SparkSession, Window, functions as F, DataFrame


def initialize_spark_session(app_name):
    return (SparkSession.builder.appName(app_name)
            .config("spark.driver.host", "127.0.0.1")  # Set driver host
            .master("local[4]")
            .getOrCreate())


def load_data(spark, file_path):
    return spark.read.json(file_path, multiLine=True)


def extract_foods_info(df):
    return (df.select(
        F.explode("foods").alias("food"))

    .select(
        F.col("food.fdcId").alias("fdcId"),
        F.col("food.description").alias("description"),
        F.col("food.dataType").alias("dataType"),
        F.col("food.foodCategory").alias("foodCategory"),
        F.col("food.publishedDate").alias("publishedDate"),
        F.explode("food.foodNutrients").alias("nutrient"))

    .select(
        F.col("fdcId"),
        F.col("description"),
        F.col("dataType"),
        F.col("foodCategory"),
        F.col("publishedDate"),
        F.col("nutrient.nutrientName").alias("nutrientName"),
        F.col("nutrient.value").alias("nutrientValue"),
        F.col("nutrient.unitName").alias("nutrientUnit")))


def process_data(foods_df: DataFrame) -> DataFrame:
    foods_df = foods_df.withColumn("publicationYear", F.year("publishedDate"))
    foods_df = foods_df.withColumn(
        "nutrientLevel",
        F.when(F.col("nutrientValue") > 50, "High")
        .when((F.col("nutrientValue") <= 50) & (F.col("nutrientValue") > 10), "Medium")
        .otherwise("Low")
    )
    windowSpec = Window.partitionBy("nutrientName")

    min_nutrient = F.min("nutrientValue").over(windowSpec)
    max_nutrient = F.max("nutrientValue").over(windowSpec)

    foods_df = foods_df.withColumn(
        "normalizedNutrientValue",
        F.when((max_nutrient - min_nutrient) != 0,
               (F.col("nutrientValue") - min_nutrient) / (max_nutrient - min_nutrient))
        .otherwise(0.0)
    )

    return foods_df


def analyse_data(foods_df):
    avg_nutrient_content = foods_df.groupBy("nutrientName", "nutrientUnit").agg(
        F.avg("nutrientValue").alias("avgNutrientValue")).orderBy("nutrientName").select(F.col('publishedDate'))

    avg_nutrient_content.show()

    median_nutrient_value = foods_df.groupBy("nutrientName", "nutrientUnit").agg(
        F.expr('percentile_approx(nutrientValue, 0.5)').alias('medianNutrientValue')).orderBy("nutrientName")

    unique_food_items_by_nutrient = foods_df.groupBy("nutrientName").agg(
        F.countDistinct("fdcId").alias("uniqueFoodItemCount")).orderBy("nutrientName")

    total_nutrient_value_per_category = foods_df.groupBy("foodCategory", "nutrientName").agg(
        F.sum("nutrientValue").alias("totalNutrientValue")).orderBy("foodCategory", "nutrientName")

    food_items_per_year = foods_df.groupBy("publicationYear").agg(
        F.count("fdcId").alias("foodItemCount")).orderBy("publicationYear")

    return {
        'avg_nutrient_content': avg_nutrient_content,
        'median_nutrient_value': median_nutrient_value,
        'unique_food_items_by_nutrient': unique_food_items_by_nutrient,
        'total_nutrient_value_per_category': total_nutrient_value_per_category,
        'food_items_per_year': food_items_per_year
    }


def write_df_to_csv(df, output_dir):
    df.write.mode("overwrite").csv(output_dir, header=True)


if __name__ == '__main__':
    spark = initialize_spark_session("USDA Food Statistics")
    file_path = "data/food_data.json"
    df = load_data(spark, file_path)
    # df.printSchema()
    foods_df = extract_foods_info(df)
    foods_df = process_data(foods_df)
    results = analyse_data(foods_df)

    output_dir_name = "output/"
    for key, df in results.items():
        write_df_to_csv(df, output_dir_name + key)
        # df.show()

    spark.stop()

# /opt/spark/bin/spark-submit   --master local[4]   spark_job.py
