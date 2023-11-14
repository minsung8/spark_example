from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName('spark-dataframe').getOrCreate()


# Spark DataFrame - csv를 불러와서 DataFrame 생성
filepath = "/home/ubuntu/working/spark-examples/data/titanic_train.csv"

# inferSchema=True : 컬럼 타입을 자동 추론
titanic_sdf = spark.read.csv(filepath, inferSchema=True, header=True)
titanic_sdf.printSchema()
titanic_sdf.show()


# Pandas DataFrame과 비교
import pandas as pd

titanic_pdf = pd.read_csv(filepath)
titanic_pdf.head()
# Spark DataFrame에서 head
titanic_sdf.head(5)


# Spark DataFrame -> Pandas DataFrame
#  분산 처리 시스템이 더이상 필요 없을 때
titanic_sdf.select("Pclass", "Embarked").toPandas() # toPandas() : Action

# describe() : 통계 정보를 확인. 판다스에서는 숫자형 컬럼에 대해서만 통계 정보가 등장
titanic_pdf.describe()

titanic_sdf.describe().show() # 문자열 형태의 컬럼에 대해서도 집계가 된다.

# 스파크 데이터프레임에서 숫자 형식의 데이터에 대한 집계
titanic_sdf.dtypes

num_cols = [column_name for column_name, dtype in titanic_sdf.dtypes if dtype != 'string']


# shape
titanic_pdf.shape

# Spark DataFrame에는 shape이 없다.
len(titanic_sdf.columns) # 컬럼의 개수 구하기

titanic_sdf.count() # 데이터의 개수(행의 개수) 구하기

"""
    Spark DataFrame의 select() 메소드 알아보기
    select 메소드는 SQL의 SELECT절과 매우 흡사하게 한 개 이상의 컬럼들의 값을 DATAFRAME 형식으로 반환
    한 개의 컬럼명 또는 여러 개의 컬럼명을 인자로 받을 수 있다.
    개별 컬럼명을 문자열 형태로 또는 DataFrame의 컬럼 속성으로 지정
    DataFrame의 컬럼 속성으로 지정할 시에는 DataFrame.컬럼명, DataFrame[컬럼명], col(컬럼명)으로 지정
"""

# Pandas 데이터 프레임에서 단일 컬럼값 가져오기
titanic_pdf['Name']

titanic_pdf[["Name", "Age"]]

# select는 Transformations
titanic_sdf.select("Name").show() # select name from titanic_sdf

titanic_sdf.select("Name", "Age").show()

# 리스트를 이용해서 Select
select_columns = ["Name", "Age","Pclass", "Fare"]

# *list 또는 list를 그대로 사용해서 데이터를 조회
#  *list : list내의 모든 원소는 unpack. *["A", "B", "C"] -> "A", "B", "C"
# 원래 select 할 때는 언팩된 형식으로 사용하는게 원칙
#  구 버전에서는 반드시 언팩이 되어있어야 했으나, 신 버전에서는 리스트를 그대로 집어 넣는 것도 지원
print(select_columns)
print(*select_columns)

titanic_sdf.select(select_columns).show() # 신 버전
titanic_sdf.select(*select_columns).show() # 구 버전

# 컬럼 속성 지정
titanic_sdf['Name']

# 브라켓 ([]) 사용하기. 이 방법은 많이 사용되는 방법이 아님
titanic_sdf.select(titanic_sdf["Name"]).show()

# 컬럼 속성을 이용한 연산
titanic_sdf["Fare"] * 100

titanic_sdf.select(
    titanic_sdf["Fare"],
    titanic_sdf["Fare"] * 100
).show()



"""
스파크 데이터프레임에서 컬럼을 다룰 때 가장 많이 사용하는 방식

col
Spark SQL에서 사용되는 모든 연산이 들어있는 패키지인 pyspark.sql.functions에서 import
"""

import pyspark.sql.functions as F

F.col("Fare") * 100

titanic_sdf.select(
    F.col("Name"),
    F.col("Fare")
).show()

titanic_sdf.select(
    F.col("Name"),
    F.upper(F.col("Name")).alias("Cap Name")
).show()

titanic_sdf.select(
    titanic_sdf.Name,
    titanic_sdf.Fare * 100
).show()


"""

    Spark DataFrame의 filter() 메소드 알아보기
    filter()는 SQL의 WHERE과 매우 흡사하게 특정 조건을 만족하는 레코드를 DataFrame으로 반환.
    filter()내의 조건 컬럼은 컬럼 속성으로 지정이 가능.
    조건문 자체는 SQL과 유사한 문자열로 지정할 수 있다.
    단, 조건 컬럼은 문자열 지정이 안된다.
    where 메소드는 filter의 alias 버전이며, where과 직관적인 동일성을 간주하기 위해 생성.
    복합 조건 and는 &를, or를 사용. 개별 조건은 괄호()로 감싸야 한다.

"""

# titanic_sdf.filter("Embarked='Q'").show()
# titanic_sdf.filter("Embarked"=='Q').show()

titanic_sdf.filter(
    F.col("Embarked") == "Q"
).show()


# where 사용하기. filter와 같다
titanic_sdf.where(
    F.col("Embarked") == "S"
).show()

# AND -> &
titanic_sdf.filter(
    (F.col("Embarked") == "S") & ( F.col("Pclass") == 1 )
).show()


# OR -> |
titanic_sdf.filter(
    ( F.col("Embarked") == "S" ) | ( F.col("Pclass") == 2 )
).show()

# Like 사용하기
titanic_sdf.filter(
    F.col("Name").like("%Miss%")
).show()

titanic_sdf.filter(
    "Name like '%Miss%'"
).show()

# 소문자 h로 시작하는 사람들의 이름을 조회하고, 이 이름을 대문자로 출력.
# 소문자화 : F.lower, 대문자화 : F.upper
titanic_sdf.filter(
    F.lower(F.col("Name")).like('h%')
).select(
    F.upper(F.col("Name"))
).show()