from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import row_number, col, when, split, regexp_replace, regexp_extract, format_number, udf
import psycopg2
import logging
import findspark
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sqlalchemy import create_engine
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import udf, hash, explode
from pyspark.sql.functions import monotonically_increasing_id, concat, lit
from pyspark.sql.types import StructType, StructField,ArrayType, StringType, IntegerType, DateType
from pyspark.sql.window import Window
from pyspark.ml.feature import NGram, Tokenizer, StopWordsRemover
# Initialize Spark session with MongoDB and PostgreSQL connectors
spark = SparkSession.builder \
.appName("Chuyển đổi MongoDB sang PostgreSQL") \
.config("spark.mongodb.input.uri", "mongodb://mongodb:27017/chothuenha_db") \
.config("spark.mongodb.input.collection", "chothuenha") \
.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.2.24") \
.getOrCreate()

logging.debug("Phiên Spark đã được khởi tạo.")

# Load data from MongoDB
df = spark.read.format("mongo").option("forceDeleteTempCheckpointLocation", "true").load()
so_luong_tai_lieu = df.count()
print(f"Số lượng tài liệu trong collection: {so_luong_tai_lieu}")
logging.debug("DataFrame đã được tạo từ MongoDB.")


# Select specific columns
df = df.select(
    "posting_id", "title", "address", "city", "district", "ward", 
    "street", "rental_price", "room_area", "bedrooms", "toilets", 
    "poster", "posting_date", "type_of_listing", "view", "describe"
)

# Create 'Street' column based on the 'Address' column if 'Street' is null
df = df.withColumn(
    "Street",
    when(
        col("Street").isNull(),
        split(col("Address"), ",")[0]  # Lấy chuỗi ký tự trước dấu ','
    ).otherwise(col("Street"))
)

# Lọc và giữ lại các dòng có giá trị không phải là null
df = df.filter(col("District").isNotNull())
df = df.filter(col("Ward").isNotNull())
df = df.filter(col("Poster").isNotNull())

# Loại bỏ từ "Đường" khỏi cột 'Street'
df = df.withColumn("Street", regexp_replace(col("Street"), "Đường", ""))
df = df.withColumn("Ward", regexp_replace(col("Ward"), "Phường", ""))
df = df.withColumn("Ward", regexp_replace(col("Ward"), "Xã", ""))
df = df.withColumn("District", regexp_replace(col("District"), "Quận", ""))
df = df.withColumn("District", regexp_replace(col("District"), "Huyện", ""))

# Trích xuất chỉ số từ cột 'Bedrooms'
df = df.withColumn("Bedrooms", regexp_extract(col("Bedrooms"), r'\d+', 0).cast("int"))

# Xử lý cột 'Room_area': Trích xuất dữ liệu số và đổi tên thành 'Room_area_m2'
df = df.withColumn(
    "Room_area_m2",
    regexp_extract(col("Room_area"), r'(\d+)', 1).cast("int")
)

# Xử lý cột 'Rental_price': Trích xuất dữ liệu số và thay thế ' Triệu'
df = df.withColumn(
    "Price_per_month",
    regexp_extract(col("Rental_price"), r'(\d+)', 1).cast("int") * 1000000
)

# Xóa cột 'Rental_price'
df = df.drop("Rental_price")

# Xóa cột 'Room_area' nếu bạn không cần nó nữa
df = df.drop("Room_area")

# Trích xuất số từ cột 'Toilets' và chuyển đổi thành kiểu số nguyên
df = df.withColumn(
    "Toilets",
    regexp_extract(col("Toilets"), r'\d+', 0).cast("int")
)

# Chuyển DataFrame PySpark sang DataFrame Pandas
pandas_df = df.toPandas()

# Điền giá trị thiếu bằng dữ liệu trước đó trong Pandas DataFrame
pandas_df['Toilets'] = pandas_df['Toilets'].fillna(method='ffill')
pandas_df['Bedrooms'] = pandas_df['Bedrooms'].fillna(method='ffill')

# Chuyển DataFrame Pandas quay lại PySpark DataFrame
df = spark.createDataFrame(pandas_df)

# Định nghĩa hàm để viết hoa chữ cái đầu tiên của mỗi từ
def capitalize_first_letter(text):
    return ' '.join(word.capitalize() for word in text.split()) if text else text

# Đăng ký hàm như một UDF
capitalize_udf = udf(capitalize_first_letter, StringType())

# Áp dụng hàm UDF lên cột 'Street'
df = df.withColumn("Street", capitalize_udf(col("Street")))

# Chọn các cột cụ thể sau khi đã xử lý
df = df.select(
    "posting_id", "title", "address", "street", "ward", "district", "city", "price_per_month",
    "room_area_m2", "bedrooms", "toilets", "poster", "posting_date", "type_of_listing", "view",
    "describe"
)
df = df.withColumnRenamed('view', 'views')
df = df.withColumnRenamed('describe', 'content')
df = df.withColumnRenamed('bedrooms', 'num_bedrooms')
df = df.withColumnRenamed('toilets', 'num_toilets')
df = df.withColumnRenamed('posting_date', 'publish_date')
df = df.withColumnRenamed('price_per_month', 'rental_price')
df = df.withColumnRenamed('room_area_m2', 'room_area')
df = df.withColumnRenamed('posting_id', 'post_id')
df = df.withColumnRenamed('type_of_listing', 'type_listing')

    # 1. Chọn các cột cần thiết và loại bỏ các giá trị trùng lặp
house_df = df.select(
    col("address").alias("address"),
    col("num_bedrooms").cast("int").alias("num_bedrooms"),
    col("num_toilets").cast("int").alias("num_toilets"),
    col("room_area").cast("int").alias("room_area"),
    col("city").alias("city"),
    col("district").alias("district"),
    col("ward").alias("ward"),
    col("street").alias("street")
).distinct()  # Loại bỏ các giá trị trùng lặp
    # Tạo cột index từ 0 trở đi
house_df = house_df.withColumn("index", monotonically_increasing_id())

# Tạo cột house_id với định dạng 'WE' và bắt đầu từ 100
house_df_with_id = house_df.withColumn(
    "house_id",
    concat(lit("H"), (col("index") + 100).cast("string"))
).drop("index")  # Bỏ cột index nếu không còn cần thiết

join_columns = ["address", "num_bedrooms", "num_toilets", "room_area", "city", "district", "ward", "street"]

# Thực hiện join
df = df.join(
    house_df_with_id,
    on=join_columns,
    how="left")

post_df = df.select(
    col("post_id").alias("post_id"),  # Không thay đổi
    col("views").cast("int").alias("views"),  # Chuyển đổi từ string sang int
    col("type_listing").alias("type_listing"),  # Không thay đổi
    col("rental_price").cast("int").alias("rental_price"),  # Chuyển đổi từ string sang float
    col("title").alias("title"),  # Không thay đổi
    col("poster").alias("poster"),  # Không thay đổi
    col("content").alias("content"),\
    col("house_id").alias("house_id")  # Không thay đổi
)    

amenity_schema = StructType([
    StructField("amenity_id", StringType(), False),
    StructField("amenity_name", StringType(), False),
    StructField("amenity_category", StringType(), False)
])

amenity_df = spark.createDataFrame([], amenity_schema)

# Dictionary amenities
amenities = {
    'Interior': ['nội thất', 'nhà bếp', 'tủ'],
    'Exterior': ['ban công', 'cửa sổ', 'có sân'],
    'Facilities': ['wifi', 'thang máy', 'máy lạnh'],
    'Location': ['mặt tiền', 'hẻm', 'gần'],
    'Pricing': ['thương lượng', 'ưu tiên', 'hợp đồng']
}
# Chuyển đổi dictionary thành list các Row
data = []
for category, names in amenities.items():
    for name in names:
        data.append(Row(amenity_name=name, amenity_category=category))

# Tạo DataFrame từ list các Row
new_amenities_df = spark.createDataFrame(data)

# Tạo cột amenity_id với định dạng 'AM' và bắt đầu từ 1000
windowSpec = Window.orderBy("amenity_name")  # Sắp xếp theo amenity_name để đảm bảo thứ tự

new_amenities_df_with_id = new_amenities_df.withColumn(
    "amenity_id",
    concat(lit("AM"), (row_number().over(windowSpec) + 999).cast("string"))
)
# # Kết hợp dữ liệu từ new_amenities_df_with_id vào amenity_df
# amenity_df = amenity_df.union(new_amenities_df_with_id)

#================== xử lý NLP==============
# Step 1: Tokenize the content
post_df = df.withColumn("content_str", concat_ws(" ", col("content")))

# Step 1: Tokenize the content
tokenizer = Tokenizer(inputCol="content_str", outputCol="words")
wordsData = tokenizer.transform(post_df)

# Custom list of Vietnamese stopwords
vietnamese_stopwords = ["và", "của", "là", "có", "trong", "với", "cho", "các", "về", "tại", "những", "một", "cái", "đã", "đến", "đi"]

# Remove Stopwords using the custom list
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=vietnamese_stopwords)
wordsData = remover.transform(wordsData)

# Step 2: Generate n-grams (unigram, bigram, trigram)
ngram1 = NGram(n=1, inputCol="filtered_words", outputCol="unigrams")
ngram2 = NGram(n=2, inputCol="filtered_words", outputCol="bigrams")
ngram3 = NGram(n=3, inputCol="filtered_words", outputCol="trigrams")

unigrams_df = ngram1.transform(wordsData)
bigrams_df = ngram2.transform(wordsData)
trigrams_df = ngram3.transform(wordsData)

def extract_amenities(ngrams):
    
    found_amenities = []
    
    # Kiểm tra từng n-gram với các từ khóa từ dictionary
    for ngram in ngrams:
        for category, keywords in amenities.items():
            for keyword in keywords:
                if keyword.lower() in ngram.lower():
                    found_amenities.append(keyword)
                    
    return list(set(found_amenities))  # Loại bỏ các giá trị trùng lặp

# Đăng ký hàm UDF
extract_amenities_udf = udf(extract_amenities, ArrayType(StringType()))

# Áp dụng UDF lên cột 'trigrams' để trích xuất amenities
amenities_df = trigrams_df.withColumn("extracted_amenities", extract_amenities_udf(col("trigrams")))

# Explode the amenities to create a row for each amenity
post_amenity_temp_df = amenities_df.select(
    col("post_id").alias("post_id"),
    explode(col("extracted_amenities")).alias("amenity_name")
).dropna()



# Join with new_amenities_df_with_id to get amenity_id
post_amenity_df = post_amenity_temp_df.join(
    new_amenities_df_with_id,  # Joining with the DataFrame that contains amenity_id
    on="amenity_name",         # Join on amenity_name column
    how="left"                 # Perform a left join to include all rows from post_amenity_temp_df
).select(
    col("post_id"),            # Select the post_id column from post_amenity_temp_df
    col("amenity_id"),         # Select the amenity_id column from new_amenities_df_with_id
    col("amenity_name")        # Select the amenity_name column from post_amenity_temp_df
)

# Define the schema with nullable=False for post_id, amenity_id, and amenity_name
post_amenity_schema = StructType([
    StructField("post_id", StringType(), nullable=False),
    StructField("amenity_id", StringType(), nullable=False),
    StructField("amenity_name", StringType(), nullable=False)
])

# Apply the schema when creating the DataFrame
post_amenity_df_with_constraints = spark.createDataFrame(post_amenity_df.rdd, schema=post_amenity_schema)

# Ghi DataFrame vào PostgreSQL
post_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_db:5432/process_data") \
    .option("dbtable", "post") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

house_df_with_id.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_db:5432/process_data") \
    .option("dbtable", "house") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

new_amenities_df_with_id.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_db:5432/process_data") \
    .option("dbtable", "amenity") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

post_amenity_df_with_constraints.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_db:5432/process_data") \
    .option("dbtable", "post_amenity") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()


