import os
import re
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType

os.environ["PYSPARK_PYTHON"] = "D:\BigData\Py\softwore\python.exe"
# 创建SparkSession
spark = SparkSession.builder \
    .appName("Rating") \
    .getOrCreate()

# 定义文件路径
input_file = "../ml-latest-small/ratings.csv"
backup_file = "rating_backup.csv"


# 备份原始文件
if os.path.exists(input_file):
    shutil.copy2(input_file, backup_file)
    print(f"已备份原始文件到: {backup_file}")
else:
    print(f"错误: 找不到输入文件 {input_file}")
    exit(1)

# 读取CSV文件
ratings_df = spark.read.csv(input_file, header=True, inferSchema=True)

# 数据类型转换
# 确保userId和movieId是整数类型
ratings_df = ratings_df.withColumn("userId", col("userId").cast(IntegerType()))
ratings_df = ratings_df.withColumn("movieId", col("movieId").cast(IntegerType()))

# 确保rating是浮点数类型
ratings_df = ratings_df.withColumn("rating", col("rating").cast(FloatType()))

# 验证rating值是否在有效范围内（0-5）
print("评分值分布:")
ratings_df.select("rating").describe().show()

# 检查是否有无效评分
invalid_ratings = ratings_df.filter((col("rating") < 0) | (col("rating") > 5))
print(f"无效评分数量: {invalid_ratings.count()}")

# 处理无效评分（可选：可以删除或修正）
# ratings_df = ratings_df.filter((col("rating") >= 0) & (col("rating") <= 5))

# 保存处理后的数据回源文件
temp_dir = "temp_ratings_csv"
ratings_df.write.csv(temp_dir, header=True, mode="overwrite")

# 查找并复制生成的CSV文件
csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]
if csv_files:
    csv_file = os.path.join(temp_dir, csv_files[0])
    shutil.copy2(csv_file, input_file)
    print(f"已将处理后的数据保存回: {input_file}")
else:
    print("错误: 未找到生成的CSV文件")

# 清理临时目录
shutil.rmtree(temp_dir)

# 停止SparkSession
spark.stop()
