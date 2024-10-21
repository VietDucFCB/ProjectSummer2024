from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, expr, regexp_replace, trim, concat_ws, collect_list, sum as F_sum
from pyspark.sql import Window

# Tạo SparkS
spark = SparkSession.builder \
    .appName("Extract_Car_Data") \
    .getOrCreate()

data_lake_path = "hdfs://localhost:9000/data_lake/data_crawled/"

# Đọc
df_raw = spark.read.option("recursiveFileLookup", "true").text(data_lake_path)

window_spec = Window.orderBy("value")
df_raw = df_raw.withColumn("is_title", df_raw['value'].contains('Title:').cast('int'))
df_raw = df_raw.withColumn("group", F_sum('is_title').over(window_spec))

# Gộp các dòng thành một dòng cho mỗi xe
df_combined = df_raw.groupBy("group").agg(concat_ws(' ', collect_list('value')).alias("value"))

patterns = {
    'Title': r'Title: (.*)',
    'Cash Price': r'Cash Price: \$(\d+,\d+)',
    'Finance Price': r'Finance Price: \$(\d+)/moEstimate',
    'Finance Details': r'Finance Details: (.*)',
    'Exterior': r'Exterior: (.*)',
    'Interior': r'Interior: (.*)',
    'Mileage': r'Mileage: ([\d,]+) miles',
    'Fuel Type': r'Fuel Type: (.*)',
    'MPG': r'MPG: (\d+ city / \d+ highway)',
    'Transmission': r'Transmission: (.*)',
    'Drivetrain': r'Drivetrain: (.*)',
    'Engine': r'Engine: (.*)',
    'Location': r'Location: (.*)',
    'Listed Since': r'Listed Since: (.*)',
    'VIN': r'VIN: (.*)',
    'Stock Number': r'Stock Number: (.*)',
}

# Tạo DataFrame mới bằng cách trích xuất các trường dựa trên biểu thức chính quy
df = df_combined
for column, pattern in patterns.items():
    df = df.withColumn(column, regexp_extract(col("value"), pattern, 1))

# Trích xuất trường "Features" bằng cách sử dụng `substring_index`
df = df.withColumn("Features", expr("substring_index(substring_index(value, 'Features: ', -1), 'See less', 1)"))

# Loại bỏ cột "value" gốc và cột "group"
df = df.drop("value").drop("group")

# In ra một số dòng để kiểm tra
df.show(truncate=False)

# Đợi quá trình ghi hoàn tất trước khi tiếp tục
spark.sparkContext.stop()
