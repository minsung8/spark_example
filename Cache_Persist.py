from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("cache_persist_example")
sc = SparkContext(conf=conf)

filepath = "/home/ubuntu/working/spark-examples/data/restaurant_reviews.csv"
lines = sc.textFile(f"file:///{filepath}")
lines.collect()

header = lines.first()

rows = lines.filter(lambda row : row != header)
rows.collect()


def parse(row):
    fields = row.split(",")
    category = fields[2]
    reviews = int(fields[3])
    return category, reviews

categoryReviews = rows.map(parse)
categoryReviews.collect()



result1 = categoryReviews.take(5)
result2 = categoryReviews.mapValues(lambda x : (x, 1)).collect()

"""
categoryReviews는 result1, result2를 수행하면서 두 개가 만들어 진다.

categoryReviews에서 데이터를 꺼내오기만 하면 되지, 변경은 일어나지 않기 때문에 persist를 이용해서 categoryReviews를 메모리에 넣어 놓는 것이 조금 더 유리할 것이다.
"""

categoryReviews = rows.map(parse).persist() # categoryReview RDD는 메모리 상에 하나만 존재하는 RDD가 된다.
result1 = categoryReviews.take(5)
result2 = categoryReviews.mapValues(lambda x : (x, 1)).collect()
