from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("key_value_rdd_op_joins")
sc = SparkContext(conf=conf)


"""
    groupByKey
    k_v_RDD.groupByKey(numPartitions=None, partitionFunc=<function portable_hash>)
    주어지는 key를 기준으로 Group을 만들어 줍니다.
    Transformations 함수 입니다.
"""

rdd = sc.parallelize([
    ("짜장면", 15),
    ("짬뽕", 10),
    ("짜장면", 5)
])

rdd.groupByKey()

# 각 그룹 별로 몇 개의 원소가 있는지 확인
rdd.groupByKey().mapValues(len).collect()

rdd.groupByKey().mapValues(list).collect()


"""
    reduceByKey
    KeyValueRDD.reduceByKey(<task>, numPartitions=None, partitionFunc=<function portable_hash>)
    주어지는 key를 기준으로 Group을 만들고 합쳐(task대로)줍니다.
    Transformations 함수 입니다.
"""

rdd.reduceByKey(lambda x, y : x + y).collect()



"""
    mapValues
    KeyValueRDD.mapValues(<task>)
    함수를 Value에만 적용합니다.
    파티션과 키는 그 위치에 그대로 있습니다.
    Transformations 
"""
rdd = sc.parallelize([
    ("하의", ["청바지", "반바지", "치마"]),
    ("상의", ["니트", "반팔", "긴팔", "나시"])
])
rdd.mapValues(len).collect()



"""
    countByKey
    KeyValueRDD.countByKey(<task>)
    각 키가 가진 요소들의 개수를 센다.
    Action
"""
rdd.countByKey()


"""
    Keys
    모든 key를 가진 RDD를 생성합니다.
    keys()는 파티션을 유지하거나 키가 굉장히 많은 경우가 있기 때문에 Transformations 입니다.
"""
rdd.keys()
rdd.keys().collect()

x.keys().count()

# 어떤 키가 있는지 중복을 제거해서 확인
x.keys().distinct().count()


"""
    Joins
    Inner Join : 서로간에 존재하는 키만 합쳐준다.
    Outer Join : 기준이 되는 한 쪽에는 데이터가 있고, 다른 쪽에는 데이터가 없는 경우
    설정한 기준에 따라서 기준에 맞는 데이터가 항상 남아있는다.
    leftOuterJoin : 왼쪽에 있는 rdd가 기준이 된다.( 함수를 호출 하는 쪽 )
    rightOuterJoin : 오른쪽에 있는 rdd가 기준이 된다. ( 함수에 매개변수로 들어가는 쪽 )
"""

rdd1 = sc.parallelize([
    ("foo", 1),
    ("goo", 2),
    ("hoo", 3)
])

rdd2 = sc.parallelize([
    ("foo", 1),
    ("goo", 2),
    ("goo", 10),
    ("moo", 6)
])

# Inner Join
rdd1.join(rdd2).collect()

# Left Outer Join. join을 호출하는 쪽이 Left, 매개변수로 들어가는 쪽이 Right
rdd1.leftOuterJoin(rdd2).collect()

# Right Outer Join
rdd1.rightOuterJoin(rdd2).collect()
