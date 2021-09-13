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
                if int(p[4]) != 0:
                    return p


texilinesCorrected = taxilines.filter(correctRows)
driver_earn = texilinesCorrected.map(lambda p: (p[1], (float(p[16]), int(p[4]))))
driver_earn = driver_earn.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
driver_earn = driver_earn.map(lambda x: (x[0], round(x[1][0]/x[1][1]/60, 3)))
print('Top 10 best drivers are:', driver_earn.top(10, lambda x: x[1]))
