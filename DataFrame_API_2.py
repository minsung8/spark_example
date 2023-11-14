from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName('spark-dataframe').getOrCreate()


filepath = "/home/ubuntu/working/spark-examples/data/titanic_train.csv"

# inferSchema=True : 컬럼 타입을 자동 추론
titanic_sdf = spark.read.csv(filepath, inferSchema=True, header=True)

import pandas as pd

titanic_pdf = pd.read_csv(filepath)

# Pandas에서는 sort_values
titanic_pdf.sort_values(by="Name", ascending=True).head()

# PClass 오름차순, Age 내림차순
titanic_pdf.sort_values(
    by=["Pclass", "Age"],
    ascending=[True, False]
)

import pyspark.sql.functions as F

titanic_sdf.orderBy(F.col("Pclass"), ascending=False).show()


# Pclass 내림차순, Age 오름차순 정렬
titanic_sdf.orderBy(
    [F.col("Pclass"), F.col("Age")],
    ascending=[False, True]
).show()


# orderBy와 동일한 메소드인 sort
titanic_sdf.sort(
    F.col("Pclass").asc(),
    F.col("Age").desc()
).show()



"""

    Agg
    Spark DataFrame은 DataFrame 객체에서 집계 메소드가 많이 없다.
    집계에 관련된 메소드는 거의 대부분 pyspark.sql.functions 패키지에 있는 함수를 활용

"""

titanic_pdf['Age'].mean()
titanic_pdf.mean()

titanic_sdf.select(
    F.col("Age")
).mean()

# 예외는 있음. Count
print("Pandas DF : \n{}".format(titanic_pdf.count())) # 각 컬럼에 대한 카운트가 나옴. NaN값은 제외하고
print()
print("Spark DF : {}".format(titanic_sdf.count())) # 전체 행의 개수만 등장.

# Spark DataFrame으로 집계하기
# from pyspark.sql.functions import max, sum, min
import pyspark.sql.functions as F

titanic_sdf_agg = titanic_sdf.select(
    F.max(F.col("Age")),
    F.min(F.col("Age")),
    F.sum(F.col("Age"))
)

titanic_sdf_agg.show()

titanic_pdf_groupby = titanic_pdf.groupby(by="Pclass")
titanic_pdf_groupby

titanic_pdf_groupby.count()

# 특정 컬럼에 특정 집계 연산 수행 - agg
agg_format = {
    "Age": "max",
    "SibSp": "sum",
    "Fare" : "mean"
}

titanic_pdf_groupby.agg(agg_format)

titanic_sdf.groupby(
    F.col("Pclass")
).count().show()

titanic_sdf.groupby(
    F.col("Pclass")
).count().orderBy(
    F.col("count"), ascending=False
).show()

# count가 아닌 다른 집계 함수를 groupby를 이용해 특정 컬럼에 적용하면 pandas 데이터프레임과 유사하게 컬럼 레벨로 집계
titanic_sdf.groupby("Pclass").max().show()

titanic_sdf.groupby("Pclass").max("Age").show()


# 집계 기준을 여러 개 적용하기
titanic_sdf.groupby(
    [F.col("Pclass"), F.col("Embarked")]
).max("Age").show()

# agg 함수 활용 시 F.col을 사용할 수 있다.
titanic_sdf.groupby("Pclass").agg(
    F.max("Age").alias("max_age"),
    F.min("Age").alias("min_age"),
    F.sum(F.col("Age")).alias("sum_age"),
    F.avg(F.col("Age")).alias("avg_age")
).show()
