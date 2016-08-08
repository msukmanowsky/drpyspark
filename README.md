# drpyspark

The doctor is in.

drpyspark provides handy utilities for debugging and tuning pyspark programs.
A work in progress.


## Better debugging

Just add

```python
import drpyspark
drpyspark.enable_debug_output()
```

To your Spark script and then you'll get wonderful output that shows you
exactly what is happening at every stage in your pyspark script without having
to add 500 print statements in between things.


```python
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

```

When run with `enable_debug_output`, provides:
```
/Users/mikesukmanowsky/code/drpyspark/examples/rdd_1.py:18: numbers = sc.parallelize([str(x) for x in xrange(10)])
['0', '1', '2', '3', '4']
/Users/mikesukmanowsky/code/drpyspark/examples/rdd_1.py:20: .map(lambda l: int(l))
[0, 1, 2, 3, 4]
/Users/mikesukmanowsky/code/drpyspark/examples/rdd_1.py:21: .map(square)
[0, 1, 4, 9, 16]
/Users/mikesukmanowsky/code/drpyspark/examples/rdd_1.py:22: .flatMap(is_even))
[0, 4, 16, 36, 64]
/Users/mikesukmanowsky/code/drpyspark/examples/rdd_1.py:23: div_100 = even_squares.map(lambda l: l / 100.0)
[0.0, 0.04, 0.16, 0.36, 0.64]
```

## Running examples

You'll need to [download a release of Apache Spark](http://spark.apache.org/)
With a virtualenv built, install `drpyspark` with `pip install drpyspark` (or
`python setup.py develop`) then run:

```
PYSPARK_PYTHON=$(which python) $SPARK_HOME/bin/spark-submit examples/<filename>
```
