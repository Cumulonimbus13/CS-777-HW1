from pyspark import SparkContext

sc = SparkContext()
lines = sc.textFile("./taxi-data-sorted-small.csv")
taxilines = lines.map(lambda x: x.split(','))


def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False


def correctRows(p):
    if len(p) == 17:
        if isfloat(p[5]) and isfloat(p[11]):
            if float(p[5]) != 0 and float(p[11]) != 0:
                return p


texilinesCorrected = taxilines.filter(correctRows)
taxi_and_driver = texilinesCorrected.map(lambda p: (p[0], 1))
taxi_and_driver = taxi_and_driver.reduceByKey(lambda x, y: x+y)
print('Top 10 active taxis are:', taxi_and_driver.top(10, lambda x: x[1]))
