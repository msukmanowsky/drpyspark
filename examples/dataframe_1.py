from __future__ import print_function
import pyspark
from pyspark.sql import functions as F
import drpyspark


drpyspark.enable_debug_output()
with pyspark.SparkContext() as sc:
    sqlContext = pyspark.sql.SQLContext(sc)
    logs = sc.parallelize([
        {'timestamp': 1470663000, 'url': 'http://example.com/', 'ip': '192.168.1.1'},
        {'timestamp': 1470663163, 'url': 'http://example.com/', 'ip': '192.168.1.1'},
        {'timestamp': 1470663277, 'url': 'http://example.com/article1', 'ip': '192.168.1.2'},
        {'timestamp': 1470663277, 'url': 'http://example.com/article2', 'ip': '192.168.1.2'},
        {'timestamp': 1470663277, 'url': 'http://example.com/article3', 'ip': '192.168.1.2'},
    ])
    logs = logs.map(lambda l: pyspark.sql.Row(**l))
    logs = (sqlContext.createDataFrame(logs)
            .withColumn('timestamp', F.to_date(F.from_unixtime('timestamp')))
            .withColumn('minute', F.date_format('timestamp', "yyyy-MM-dd'T'HH")))
    (logs
     .groupBy(['minute', 'url'])
     .count()
     .show())
