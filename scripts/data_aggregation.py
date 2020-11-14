# The script was used within Zeppelin and so it relies on Zeppelin functionalities

# import Spark functions
import pyspark.sql.functions as F

# read input data delivered by Refinitiv
input_data = spark.read.csv(r'gs://notebooks_bucket/PATH_TO_SAMPLE_DATA_PATH.csv.gz', header=True)

# take a look at the data: show count, mean, stddev, max, and min values per column
df_stats = input_data.sample(True, 0.05).describe()
z.show(df_stats)

# Select relevant and non-empty fields following a first check on Zeppelin
# Take only a 20% sample due to cluster limitations
df_summary = input_data.sample(True, 0.2).select(
    'column_1', 'column_2', 'column_N'
    )

# Get names of string fields and numerical fields
fields = df_summary.dtypes
num_fields = [f[0] for f in fields if f[1] in ['float', 'int', 'long']]
string_fields = [f[0] for f in fields if f[1] in ('string')]

# Perform aggregations related to string columns and write data to gs bucket
for f in string_fields:
    df_count = df_summary.select(f).groupBy(f).count().orderBy(F.desc('count')).limit(1000)
    df_post_count = df_summary.join(df_count, f, "inner").drop('count')
    df_count.coalesce(1).write.option("header","true").csv('gs://notebooks_bucket/aggregated_data/columns/'+f+'/count/')
    df_sum = df_post_count.select(f, num_fields).groupBy(f).sum().orderBy(F.desc(num_fields)).limit(1000)
    df_sum.coalesce(1).write.option("header","true").csv('gs://notebooks_bucket/aggregated_data/columns/'+f+'/sum/')
    df_mean = df_post_count.select(f, num_fields).groupBy(f).mean().orderBy(F.desc(num_fields)).limit(1000)
    df_mean.coalesce(1).write.option("header","true").csv('gs://notebooks_bucket/aggregated_data/columns/'+f+'/mean/')
