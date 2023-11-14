from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName('spark-dataframe').getOrCreate()

filepath = "/home/ubuntu/working/spark-examples/data/titanic_train.csv"

# inferSchema=True : 컬럼 타입을 자동 추론
titanic_sdf = spark.read.csv(filepath, inferSchema=True, header=True)

import pandas as pd

titanic_pdf = pd.read_csv(filepath)


import numpy as np

titanic_pdf_copy = titanic_pdf.copy()

# Fare에 10일 곱해서 Extra_Fare 컬럼에 집어 넣기
titanic_pdf_copy["Extra_Fare"] = titanic_pdf_copy["Fare"] * 10
titanic_pdf_copy.head()


# 데이터 수정. Fare에 20 더하기
titanic_pdf_copy["Fare"] = titanic_pdf_copy["Fare"] + 20
titanic_pdf_copy.head()


"""

    Spark Dataframe 데이터 조작
    withColumn() 메소드를 이용하여 기존 컬럼 값을 수정, 타입 변경, 신규 컬럼을 추가한다.
    withColumn('신규 또는 업데이트 되는 컬럼명', '신규 또는 업데이트 되는 값')
    신규 또는 업데이트 되는 값을 생성 시에 기존 컬럼을 기반으로 한다면,
    신규 컬럼은 문자열로 지정
    기존 컬럼은 col 을 사용한다.
    신규 컬럼을 추가하는 것은 select로도 가능
    컬럼명 변경은 withColumnRename() 메소드 사용

"""

from pyspark.sql.functions import col

titanic_sdf_copy = titanic_sdf.select("*")

titanic_sdf_copy = titanic_sdf_copy.withColumn("Extra_Fare", col("Fare") * 10)
titanic_sdf_copy.select("Fare", "Extra_Fare").show(5)


# 기존 컬럼 Update
titanic_sdf_copy = titanic_sdf_copy.withColumn("Fare", col("Fare") + 20)
titanic_sdf_copy.select("Fare").show(5)

# 컬럼 타입 변환
titanic_sdf_copy = titanic_sdf_copy.withColumn("Fare", col("Fare").cast("Integer"))
titanic_sdf_copy.select("Fare").show(5)

# pandas에서 리터럴로 데이터를 삽입하거나 수정하기.
titanic_pdf_copy["Extra_Fare"] = 10
titanic_pdf_copy.head()


# spark에서 리터럴로 데이터를 삽입하거나 수정하기
titanic_sdf_copy = titanic_sdf_copy.withColumn("Extra_Fare", 10)


# 상숫값으로 업데이트 하려면 반드시 lit 함수를 사용
from pyspark.sql.functions import lit

titanic_sdf_copy = titanic_sdf_copy.withColumn("Extra_Fare", lit(10))
titanic_sdf_copy.select("Extra_Fare").show(5)


# select절을 활용하여 컬럼 업데이트 하기
from pyspark.sql.functions import substring

# 컬럼 추가
# SQL : select *, Embakred as E from titanic_sdf_copy;
titanic_sdf_copy = titanic_sdf_copy.select("*", col("Embarked").alias("E"))
titanic_sdf_copy.show(5)

# Cabin에서 첫 알파벳 글자만 추출해서 Cabin_Section컬럼 추가
# substring 사용.
titanic_sdf_copy = titanic_sdf_copy.select("*", substring("Cabin", 0, 1).alias("Cabin_Section"))
titanic_sdf_copy.show(5)


from pyspark.sql.functions import split

# split(컬럼명, 나눌 문자)

# first_name 컬럼과 last_name컬럼 추가. withColumn 사용하기
#  split 하고 나서 getItem(0), getItem(1)
titanic_sdf_copy = titanic_sdf_copy\
                    .withColumn("first_name", split(col("Name"), ",").getItem(0))\
                    .withColumn("last_name", split(col("Name"), ",").getItem(1))

titanic_sdf_copy.select("first_name","last_name").show(5)


# 컬럼 이름 변경하기
titanic_sdf_copy.withColumnRenamed("Fare", "요금").printSchema()


titanic_sdf_copy.withColumnRenamed("없는 컬럼 이름 아무거나 넣어도 에러가 안나요", "요금").printSchema()


"""

    Spark Dataframe의 컬럼, 로우(레코드) 삭제
    pandas의 데이터프레임은 drop 메소드를 사용. 행과 열 모두 삭제
    spark 데이터프레임에도 drop 메소드를 사용. 컬럼만 삭제 가능
    여러 개의 컬럼을 삭제 할 때 리스트 사용 불가
    spark에서는 데이터(row)의 삭제가 원칙적으로는 불가능.
    데이터 삭제가 없는 대신에 filter를 이용해서 필요한 것만 추출

"""


titanic_pdf_drop = titanic_pdf.drop("Name", axis=1)
titanic_pdf_drop.info()

# drop을 사용한다고 해서 실제 삭제가 이루어지진 않는다.
#  drop으로 지정된 컬럼을 제외하고 select한 것!
titanic_sdf_drop = titanic_sdf.drop(col("Name"))
titanic_sdf_drop.printSchema()


# Pclass가 1인 row를 제거. 하지만 실제로는 삭제가 아닌 Pclass != 1 인 데이터만 가져오는 것
titanic_removed_pclass_1 = titanic_sdf.filter(col("Pclass") != 1)
titanic_removed_pclass_1.show(5)

# 레코드에 하나라도 Null 또는 NaN값이 있으면 삭제한 결과의 DataFrame이 반환
#  행만 삭제 된다.
print("DropNa 이전의 행의 개수 :", titanic_sdf.count())

titanic_sdf_dropna_1 = titanic_sdf.dropna()
print("dropna 이후의 행의 개수 :", titanic_sdf_dropna_1.count())


# 특정 컬럼을 지정하여 거기에 Null이 있는 경우에만 삭제
titanic_sdf_dropna_2 = titanic_sdf.dropna(subset=["Age", "Embarked"])
titanic_sdf_dropna_2.count()


# dropna 대신 na 를 사용할 수 도 있다.
titanic_sdf_dropna_3 = titanic_sdf.na.drop()
titanic_sdf_dropna_3.count()

# filter 사용하기
# isNotNull()

titanic_sdf.filter(
    col("Age").isNotNull() & col("Embarked").isNotNull()
).count()


# dropna() 메소드를 WHERE절 로직으로 구현. 
where_str = ''
column_count = len(titanic_sdf.columns)
for index, column_name in enumerate(titanic_sdf.columns):
    where_str += (column_name +' IS NOT NULL ') 
    if index < column_count - 1:
        where_str += 'and '
print(where_str)

titanic_sdf.filter(where_str).count()


a = None
print(type(a), a)


titanic_sdf.select("Age", "Cabin").show(10)

titanic_pdf = titanic_sdf.select("*").toPandas()
titanic_pdf[["Age", "Cabin"]].head(10)

# pandas 1.x 버전에서만 사용이 가능한 코드. pandas 2.x과 Spark가 호환이 잘 안됨
# spark 3.2 버전에서는 NaN과 None을 전부 null로 만들어 준다.

titanic_sdf_from_pdf = spark.createDataFrame(titanic_pdf)
titanic_sdf_from_pdf.select("Age", "Cabin").show(10)

titanic_pdf[["Age", "Cabin"]].isna().head(10)

from pyspark.sql.functions import isnull # 사용 하지 않는 것을 추천

titanic_sdf.filter(col("Age").isNull()).select("Name", "Age").show(10)
titanic_sdf.filter(isnull(col("Age"))).select("Name", "Age").show(10)

null_columns = [col(column_name).isNull() for column_name in titanic_sdf.columns ]

titanic_sdf.select(null_columns).show(10)

# pandas에서 누락값 개수 확인하기
titanic_pdf.isna().sum()

from pyspark.sql.functions import count, when

null_count_condition = [ count(when(col(c).isNull(), c)).alias(c) for c in titanic_sdf.columns ]
null_count_condition

titanic_sdf.select(null_count_condition).show()

titanic_pdf.info()
titanic_pdf["Age"] = titanic_pdf['Age'].fillna(titanic_pdf['Age'].mean())
titanic_pdf.info()

titanic_pdf['Age'].mean()

# 스파크 데이터프레임에서 결측치 채우기
titanic_sdf.fillna(value=999).select("Age").show(10)

titanic_sdf.fillna("NA").select("Age", "Cabin").show(10)
titanic_sdf.fillna(999).select("Age", "Cabin").show(10)


# Age에 대한 결측치를 Age의 평균으로 처리
from pyspark.sql.functions import avg

avg_age = titanic_sdf.select(avg(col("Age")))
avg_age.show()
type(avg_age)


# fillna 수행 시에 value에 데이터 프레임을 넣을 수 없다.
titanic_sdf.fillna(value=avg_age, subset=["Age"])

# 데이터 프레임을 RDD 조회 결과로 확인
avg_age_row = avg_age.collect()
avg_age_row

avg_age_value = avg_age_row[0]
avg_age_value = avg_age_row[0][0]

titanic_sdf.fillna(value=avg_age_value, subset=["Age"]).select("Age").show()
