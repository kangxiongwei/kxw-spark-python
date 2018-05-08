from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf()
conf.setMaster('spark://localhost:7077').setAppName('Python Spark App')

sc = SparkContext('spark://localhost:7077', 'Python Spark App')

# 利用文件创建RDD
lines = sc.textFile("file:///Users/kangxiongwei/PycharmProjects/kxw-spark/src/README.MD")
size = lines.count()
print("总共%d行记录" % size)
# 循环打印每行内容
print('--------lines start----------')
for line in lines.take(size):
    print(line)
print('--------lines end------------')

# collect用法，不推荐，除非数据量比较少
# coll = lines.collect()
# print(coll)

# 保存文件，一般会将RDD存到HDFS或S3这种分布式存储集群中，默认保存到HDFS上
# lines.saveAsTextFile("/spark.txt")

# 利用序列创建RDD
'''
pall = sc.parallelize(['zhangsan', 'lisi', 'wangwu'])
print(pall.first())
'''


# 传递函数
def contains_error(s):
    return "hello" in s


print('--------------------------RDD常见的转换操作---------------------------------')
# map()
numbers = sc.parallelize([1, 2, 3, 3]).persist()
print(numbers.map(lambda x: x + 1).collect())
# flatMap(), distinct(), collect()
words = sc.parallelize(['hello world', 'hello spark', 'hello python'])
ws = words.flatMap(lambda x: x.split(" ")).distinct().collect()
print(ws)
# filter()
word = lines.filter(contains_error)
# filter操作
python = lines.filter(lambda x: "python" in x)
spark = lines.filter(lambda x: "spark" in x)
# 并集union()
pythonAndSpark = python.union(spark)
for line in pythonAndSpark.take(pythonAndSpark.count()):
    print(line)
# 交集intersection()
pythonFlatMap = python.flatMap(lambda x: x.split(" ")).persist()
sparkFlatMap = spark.flatMap(lambda x: x.split(" ")).persist()
intersection = pythonFlatMap.intersection(sparkFlatMap)
print(intersection.first())
# A和B的差集subtract()
st = pythonFlatMap.subtract(sparkFlatMap)
print(st.collect())
# A和B的笛卡尔积
ct = pythonFlatMap.cartesian(sparkFlatMap)
print(ct.collect())

print('--------------------------RDD常见的动作函数---------------------------------')
print(numbers.reduce(lambda x, y: x + y))
