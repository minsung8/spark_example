from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("restaurant-review-average")
sc = SparkContext(conf=conf)

filepath="/home/ubuntu/working/spark-examples/data/restaurant_reviews.csv"

lines = sc.textFile(f"file:///{filepath}")
lines.collect()

# header 제거하기
header = lines.first()

rows = lines.filter(lambda row : row != header)
rows.collect()


# 각 행을 파싱하기 위한 함수
def parse(row):
    fields = row.split(",")
    
    # 카테고리 항목 얻어오기
    category = fields[2]
    
    # 리뷰 개수 얻어오기
    reviews = fields[3]
    reviews = int(reviews)
    
    return category, reviews


# RDD 내의 모든 원소(row)에 대해 parse 함수를 적용하고 추출(map)
category_reviews = rows.map(parse)
category_reviews


# 카테고리 리뷰를 구하기위한 데이터 생성
#  - 각 카테고리별 리뷰의 개수
#  - 각 카테고리의 종류의 개수
category_reviews_count = category_reviews.mapValues(lambda reviews : (reviews, 1))
category_reviews_count.collect()


reduce = category_reviews_count.reduceByKey(lambda x, y : (x[0] + y[0], x[1] + y[1]))
reduce.collect()

average = reduce.mapValues(lambda x : x[0] / x[1])
average.collect()
