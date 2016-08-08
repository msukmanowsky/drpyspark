from __future__ import print_function
import pyspark
import drpyspark
drpyspark.enable_debug_output()


def is_even(val):
    if val % 2 == 0:
        return [val]
    return []


def square(val):
    return val ** 2


with pyspark.SparkContext() as sc:
    numbers = sc.parallelize([str(x) for x in xrange(10)])
    even_squares = (numbers
                    .map(lambda l: int(l))
                    .map(square)
                    .flatMap(is_even))
    div_100 = even_squares.map(lambda l: l / 100.0)
    print(div_100.collect())

