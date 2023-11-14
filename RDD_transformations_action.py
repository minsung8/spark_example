from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("transformations_actions")
sc = SparkContext(conf=conf)


# 일반 파이썬의 리스트를 이용해서 RDD 생성
foods = sc.parallelize([
    "짜장면", "짬뽕", "마라탕",
    "떡볶이", "쌀국수","짬뽕",
    "짜장면", "짜장면", "짬뽕",
    "마라탕", "라면", "라면",
    "우동", "쌀국수"
])
foods.collect()


# 각 원소 별 개수 세기
foods.countByValue()


# 상위 n개의 데이터 가져오기
foods.take(5)


# RDD 내 전체 데이터에 대한 개수 세기
foods.count()


"""
arrow Transformations
1:1 변환을 의미한다.
하나의 열을 조작하기 위해서 다른 열 및 파티션의 데이터를 사용하지 않는다.

`map(<task>)`
- 데이터를 하나씩 꺼내서 `<task>`가 적용된 새로운 RDD가 만들어 진다.
"""

sample_rdd = sc.parallelize([1, 2, 3])
# task 함수는 반드시 리턴이 있어야 한다.
sample_rdd2 = sample_rdd.map(lambda x : x * 2)


# map 함수와 매우 흡사하나, flatMap 함수는 map의 모든 결과를 1차원 배열 형식으로 평평하게(flat) 나타낸다.
movies = [
    "그린 북",
    "매트릭스",
    "토이 스토리",
    "캐스트 어웨이",
    "포드 V 페라리",
    "보헤미안 랩소디",
    "빽 투 더 퓨처",
    "반지의 제왕",
    "죽은 시인의 사회"
]

moviesRDD = sc.parallelize(movies)
moviesRDD.collect()

mapMovies = moviesRDD.map(lambda x : x.split())
mapMovies.collect()

flatMapMovies = moviesRDD.flatMap(lambda x : x.split())
flatMapMovies.collect()


# filter(<task>) : task의 결과가 True인 데이터만 추출
filtered_movies = flatMapMovies.filter(lambda x : x != '매트릭스')
filtered_movies.collect()


num1 = sc.parallelize([1, 2, 3, 4, 5])
num2 = sc.parallelize([4, 5, 6, 7, 8, 9, 10])

# 교집합 - intersection
interRDD = num1.intersection(num2)
interRDD.collect()

# 합집합 - union
unionRDD = num1.union(num2)
unionRDD.collect()

# 차집합 - subtract
subRDD = num1.subtract(num2) # num1 - num2
subRDD.collect()

num2.subtract(num1).collect() # num2 - num1
