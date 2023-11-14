from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("spark_sql_basic")
sc = SparkContext(conf=conf)

movies_rdd = sc.parallelize([
    (1, ("어벤져스", "마블")),
    (2, ("슈퍼맨", "DC")),
    (3, ("배트맨", "DC")),
    (4, ("겨울왕국", "디즈니")),
    (5, ("아이언맨", "마블"))
])

attendances_rdd = sc.parallelize([
    (1, (13934592, "KR")),
    (2, (2182227,"KR")),
    (3, (4226242, "KR")),
    (4, (10303058, "KR")),
    (5, (4300365, "KR"))
])

# 마블 영화 중 관객 수가 500만 이상인 영화를 가져오기

# CASE 1 : Join 먼저, Filter 나중에
movie_att_rdd = movies_rdd.join(attendances_rdd)
movie_att_rdd.filter(lambda x : x[1][0][1] == '마블' and x[1][1][0] >= 5000000).collect()

# CASE 2 : Filter 먼저, Join 나중에
filtered_movies = movies_rdd.filter(lambda x : x[1][1] == "마블")
filtered_attendances = attendances_rdd.filter(lambda x : x[1][0] > 5000000)
filtered_movies.join(filtered_attendances).collect()


# SparkContext에 해당하며, 새로운 스파크 어플리케이션을 만들어준다.
from pyspark.sql import SparkSession
spark = (SparkSession.builder
            .master("local")
            .appName("spark-sql")
            .getOrCreate())

movies = [
    (1, "어벤져스", "마블", 2012, 4, 26),
    (2, "슈퍼맨", "DC", 2013, 6, 13),
    (3, "배트맨", "DC", 2008, 8, 6),
    (4, "겨울왕국", "디즈니", 2014, 1, 16),
    (5, "아이언맨", "마블", 2008, 4, 30)
]
movie_schema = ["id", "name", "company", "year", "month", "day"]

# 데이터 프레임 만들기
df = spark.createDataFrame(data=movies, schema=movie_schema)

# 스키마 타입 확인
df.dtypes

# 데이터 프레임 내용 출력
df.show()



"""
    Spark SQL 사용하기
    createOrReplaceTempView 함수를 이용해서 DataFrame에 SQL을 날릴 수 있는 View를 만들어준다.
"""

df.createOrReplaceTempView("movies")

query = """
SELECT name
FROM movies
"""

# 쿼리 실행
spark.sql(query).show()

# 영화 이름, 개봉 연도 가져오기
query = """
SELECT name, year
FROM movies
"""

# 쿼리 실행
spark.sql(query).show()

# 2010년도 이후에 개봉한 영화의 모든 정보
query = """
SELECT *
FROM movies
WHERE year > 2010
"""

spark.sql(query).show()


# 2010년도 이후에 개봉한 마블 영화의 모든 정보
query = """
SELECT *
FROM movies
WHERE year > 2010
  AND company='마블'
"""

spark.sql(query).show()


# ~맨으로 끝나는 영화의 모든 정보
query = """
SELECT *
FROM movies
WHERE name like '%맨'
"""

spark.sql(query).show()

# id가 3번인 영화보다 늦게 개봉한 마블영화의 모든 정보(연도만 고려)
query = """
SELECT *
FROM movies
WHERE company='마블'
  AND year > (SELECT year FROM movies WHERE id=3)
"""
spark.sql(query).show()


# Join 구현

attendances = [
    (1, 13934592., "KR"),
    (2, 2182227.,"KR"),
    (3, 4226242., "KR"),
    (4, 10303058., "KR"),
    (5, 4300365., "KR")
]
# 자료형 타입 불러오기(컬럼 타입)
from pyspark.sql.types import StringType, FloatType, IntegerType

# 구조를 만들기 위한 타입 불러오기(필수)
from pyspark.sql.types import StructField, StructType

# 만들어낼 데이터 프레임에 대한 스키마 정의
att_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('attendance', FloatType(), True),
    StructField("theater_country", StringType(), True)
])

att_df = spark.createDataFrame(data=attendances, schema=att_schema)

att_df.createOrReplaceTempView("att")

query = """
SELECT *
FROM movies
JOIN att ON movies.id = att.id
ORDER BY movies.id ASC
"""

spark.sql(query).show()
