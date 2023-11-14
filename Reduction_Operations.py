from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("reduction-op")
sc = SparkContext(conf=conf)

"""
    Reduce
    RDD.reduce(<task>)
    사용자가 지정하는 task를 받아 여러 개의 값을 하나로 줄여준다.   
"""

from operator import add
sample_rdd = sc.parallelize([1, 2, 3, 4, 5]).reduce(add) # add : lambda x, y : x + y

# ## 파티션에 따라 결과물이 달라진다. - 분산된 파티션들의 연산과 합치는 부분을 나눠서 생각해야 함

# 파티션이 1개인 경우
sc.parallelize([1, 2, 3, 4]).reduce(lambda x, y : (x * 2) + y)

# 파티션이 2개인 경우
sc.parallelize([1, 2, 3, 4], 2).reduce(lambda x, y : (x * 2) + y)

# 파티션이 3개인 경우
sc.parallelize([1, 2, 3, 4], 3).reduce(lambda x, y : (x * 2) + y)

# 파티션이 4개인 경우
sc.parallelize([1, 2, 3, 4], 4).reduce(lambda x, y : (x * 2) + y)

"""
    Fold
    RDD.fold(zeroValue, <task>)
    reduce와 비슷하지만, 시작값을 zeroValue에 넣어놓고 reduce할 수 있다.
"""

rdd = sc.parallelize([2, 3, 4], 4)

print(rdd.reduce(lambda x, y : (x * y))) # 2 * 3 * 4
print(rdd.fold(1, lambda x, y : (x * y))) # 1(zeroValue * zeroValue) * 2(zeroValue * 2) * 3(zeroValue * 3) * 4(zeroValue * 4)

print(rdd.reduce(lambda x, y : x + y)) # 2 + 3 + 4
print(rdd.fold(1, lambda x, y : x + y)) # (1+1) + (1+2) + (1+3) + (1+4)


"""
    GroupBy
    RDD.groupBy(<task>)
    그루핑 함수를 받아서 그룹의 기준에 맞게 데이터가 Reduction 된다
"""

rdd = sc.parallelize([1, 1, 2, 3, 5, 8, 10, 1,123,12,56,7,123,123,5,56,1236,8,8,45,657])

# 홀수, 짝수 별로 그룹핑
result = rdd.groupBy(lambda x : x % 2).collect()
sorted([(x, sorted(y)) for (x, y) in result])


"""
    Aggregate( Action )
    RDD.aggregate(zeroValue, seqOp, combOp)
    zeroValue : 각 파티션에서 누적할 시작값
    seqOp : 타입 변경 함수. 파티션 내부에서 사용할 task
    combOp : 합치는 함수. 파티션 끼리 사용할 task
"""

rdd = sc.parallelize([1, 2, 3, 4], 2)
seqOp = (lambda x, y: (x[0] + y, x[1] + 1)) # 파티션 내의 연산
combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1])) # 파티션의 모든 결과를 최종 연산

rdd.aggregate((0, 0), seqOp, combOp)
