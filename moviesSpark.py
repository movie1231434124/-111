import re
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, split
from pyspark.sql.types import StringType, IntegerType

import os
from pyspark.sql import SparkSession

# 设置 Python 解释器路径
# os.environ["PYSPARK_PYTHON"] = ""
os.environ["PYSPARK_PYTHON"] = "D:\BigData\Py\softwore\python.exe"
# 创建SparkSession
spark = SparkSession.builder \
    .appName("Movies Data Preprocessing") \
    .getOrCreate()

# 定义文件路径
input_file = "../ml-latest-small/movies.csv"
backup_file = "movies_backup.csv"

# 备份原始文件
if os.path.exists(input_file):
    shutil.copy2(input_file, backup_file)
    print(f"已备份原始文件到: {backup_file}")
else:
    print(f"错误: 找不到输入文件 {input_file}")
    exit(1)

# 读取CSV文件
movies_df = spark.read.csv(input_file, header=True, inferSchema=True)

# 定义UDF函数从标题中提取年份
def extract_year(title):
    year_match = re.search(r'\((\d{4})\)', title)
    if year_match:
        return int(year_match.group(1))
    return None

# 注册UDF
extract_year_udf = udf(extract_year, IntegerType())

# 应用UDF提取年份并创建新列
movies_df = movies_df.withColumn("year", extract_year_udf(col("title")))

# 清理标题（移除年份）
def clean_title(title):
    return re.sub(r'\(\d{4}\)', '', title).strip()

clean_title_udf = udf(clean_title, StringType())
movies_df = movies_df.withColumn("title", clean_title_udf(col("title")))

# 保存处理后的数据回源文件
# 先保存到临时目录
temp_dir = "temp_movies_csv"
movies_df.write.csv(temp_dir, header=True, mode="overwrite")

# 查找临时目录中的CSV文件（Spark会创建多个分区文件）
csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]
if not csv_files:
    print("错误: 未找到生成的CSV文件")
    exit(1)

# 合并CSV文件（简单情况下只取第一个文件）
# 注意：在生产环境中可能需要合并多个分区文件
csv_file = os.path.join(temp_dir, csv_files[0])

# 覆盖原始文件
shutil.copy2(csv_file, input_file)
print(f"已将处理后的数据保存回: {input_file}")

# 清理临时目录
shutil.rmtree(temp_dir)

# 停止SparkSession
spark.stop()
