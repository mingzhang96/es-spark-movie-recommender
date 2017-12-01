"""
A recommender using elasticsearch and spark
"""

import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import tmdbsimple as tmdb
from elasticsearch import Elasticsearch
from pyspark.ml.recommendation import ALS
import datetime, time

# your key
tmdb.API_KEY = 'YOUR KEY'
# your index name
indexName = "demo"

conf = pyspark.SparkConf().setMaster("local").setAppName("My App").set("es.nodes", "127.0.0.1").set("es.port", "9200") \
    .set("es.mapping.date.rich", "false")
sc = pyspark.SparkContext(conf=conf)
sql = pyspark.SQLContext(sc)

PATH_TO_DATA = "./data/ml-latest-small"
PATH_TO_RATINGS = PATH_TO_DATA + "/ratings.csv"
PATH_TO_MOVIES = PATH_TO_DATA + "/movies.csv"
PATH_TO_TAGS = PATH_TO_DATA + "/tags.csv"
PATH_TO_LINKS = PATH_TO_DATA + "/links.csv"

# =====process rating======
ratings = sql.read.format("com.databricks.spark.csv").option("header", "true").load(PATH_TO_RATINGS)

print("Number of ratings: %i" % ratings.count())
ratings.show(5)

timestampUdf = udf(lambda x: str(datetime.datetime.fromtimestamp(int(x))), StringType())
ratings = ratings.withColumn("timestamp", timestampUdf(ratings.timestamp))
print("after *1000: ")
ratings.show(5)
# ===========end============

# ======process movies======
movies = sql.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(
    PATH_TO_MOVIES)
movies.show(5, False)

extract_genres = udf(lambda row: row.lower().split("|"), ArrayType(StringType()))

print("raw movies sample: ")
movies.show(5, False)


# define a UDF to extract the release year from the title, and return the new title and year in a struct type
def extract_year_fn(title):
    result = re.search("\(\d{4}\)", title)
    try:
        if result:
            group = result.group()
            year = group[1:-1]
            start_pos = result.start()
            title = title[:start_pos - 1]
            return (title, year)
        else:
            return (title, 1970)
    except:
        print(title)


extract_year = udf(extract_year_fn, StructType(
    [StructField("title", StringType(), True), StructField("release_date", StringType(), True)]))

movies = movies.select("movieId", extract_year("title").title.alias("title"),
                       extract_year("title").release_date.alias("release_date"),
                       extract_genres("genres").alias("genres"))
print("Cleaned movie data:")
movies.show(5, truncate=False)
# ===========end============

# ======process links ======
link_data = sql.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(
    PATH_TO_LINKS)
# join movies with links to get TMDB id
movie_data = movies.join(link_data, movies.movieId == link_data.movieId).select(movies.movieId, movies.title,
                                                                                movies.release_date, movies.genres,
                                                                                link_data.tmdbId)
print("Cleaned movie data with tmdbId links:")
movie_data.show(5, truncate=False)
# ===========end============


# =======load into es=======
# test your ES instance is running
es = Elasticsearch()
print(es.info(pretty=True))

create_index = {
    "settings": {
        "analysis": {
            "analyzer": {
                # this configures the custom analyzer we need to parse vectors such that the scoring
                # plugin will work correctly
                "payload_analyzer": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filter": "delimited_payload_filter"
                }
            }
        }
    },
    "mappings": {
        "ratings": {
            # this mapping definition sets up the fields for the rating events
            "properties": {
                "timestamp": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "userId": {
                    "type": "integer"
                },
                "movieId": {
                    "type": "integer"
                },
                "rating": {
                    "type": "double"
                }
            }
        },
        "users": {
            # this mapping definition sets up the metadata fields for the users
            "properties": {
                "userId": {
                    "type": "integer"
                },
                "@model": {
                    # this mapping definition sets up the fields for user factor vectors of our model
                    "properties": {
                        "factor": {
                            "type": "text",
                            "term_vector": "with_positions_offsets_payloads",
                            "analyzer": "payload_analyzer"
                        },
                        "version": {
                            "type": "keyword"
                        },
                        "timestamp": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss"
                        }
                    }
                }
            }
        },
        "movies": {
            # this mapping definition sets up the metadata fields for the movies
            "properties": {
                "movieId": {
                    "type": "integer"
                },
                "tmdbId": {
                    "type": "keyword"
                },
                "genres": {
                    "type": "keyword"
                },
                "release_date": {
                    "type": "date",
                    "format": "year"
                },
                "@model": {
                    # this mapping definition sets up the fields for movie factor vectors of our model
                    "properties": {
                        "factor": {
                            "type": "text",
                            "term_vector": "with_positions_offsets_payloads",
                            "analyzer": "payload_analyzer"
                        },
                        "version": {
                            "type": "keyword"
                        },
                        "timestamp": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss"
                        }
                    }
                }
            }
        }
    }
}
# create index with the settings and mappings above
if not es.indices.exists(index=indexName):
    es.indices.create(index=indexName, body=create_index)

# write ratings data
ratings.write.format("es").save(indexName + "/ratings")
# ratings.write.format("org.elasticsearch.spark.sql").option("es.resource", indexName+"/ratings")\
#     .option("es.nodes", "localhost").save()

# check write went ok
print("ratings DF count: %d" % ratings.count())
print("ratings ES count:  %d" % es.count(index=indexName, doc_type="ratings")['count'])

# test things out by retrieving a few rating event documents from Elasticsearch
print(es.search(index=indexName, doc_type="ratings", q="*", size=3))

print(es.count(index=indexName, doc_type="ratings", body={"query": {"bool": {"must": {"range": {
    "timestamp": {"gte": "2016-01-01 00:00:00", "lt": "2016-02-01 00:00:00", "format": "yyyy-MM-dd HH:mm:ss"}}}}}}))

# write movie data, specifying the DataFrame column to use as the id mapping
movie_data.write.format("es").option("es.mapping.id", "movieId").save(indexName+"/movies")
# movie_data.write.format("org.elasticsearch.spark.sql").option("es.resource", indexName + "/movies")\
#     .option("es.nodes", "localhost").save()

# check load went ok
print("movies DF count: %d" % movie_data.count())
print("movies ES count: %d" % es.count(index=indexName, doc_type="movies")['count'])

# test things out by searching for movies containing "matrix" in the title
print(es.search(index=indexName, doc_type="movies", q="title:matrix", size=3))

# =========train a model on the ratings data=========

ratings_from_es = sql.read.format("es").load(indexName + "/ratings")
ratings_from_es.show(5, False)

als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", regParam=0.01, rank=20, seed=12)
model = als.fit(ratings_from_es)
print("model -- userFactors: ")
model.userFactors.show(5)
print("model -- itemFactors: ")
model.itemFactors.show(5)


# ==========finish training model==========


# ==========export ALS user and item factor vectors to ES===========
def convert_vector(x):
    '''Convert a list or numpy array to delimited token filter format'''
    return " ".join(["%s|%s" % (i, v) for i, v in enumerate(x)])


def reverse_convert(s):
    '''Convert a delimited token filter format string back to list format'''
    return [float(f.split("|")[1]) for f in s.split(" ")]


def vector_to_struct(x, version, ts):
    '''Convert a vector to a SparkSQL Struct with string-format vector and version fields'''
    return convert_vector(x), version, ts


vector_struct = udf(vector_to_struct, \
                    StructType([StructField("factor", StringType(), True), \
                                StructField("version", StringType(), True),\
                                StructField("timestamp", StringType(), True)]))

ver = model.uid
ts = str(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
movie_vectors = model.itemFactors.select("id", vector_struct("features", lit(ver), lit(ts)).alias("@model"))
movie_vectors.select("id", "@model.factor", "@model.version", "@model.timestamp").show(5, False)
movie_vectors.select("id", "@model").show(5, False)

user_vectors = model.userFactors.select("id", vector_struct("features", lit(ver), lit(ts)).alias("@model"))
user_vectors.select("id", "@model.factor", "@model.version", "@model.timestamp").show(5, False)

'''
movie_data.write.format("org.elasticsearch.spark.sql").option("es.resource", indexName + "/movies")
    .option("es.nodes", "localhost").save()
'''

# write data to ES, use:
# - "id" as the column to map to ES movie id
# - "update" write mode for ES, since you want to update new fields only
# - "append" write mode for Spark

# movie_vectors.write.format("org.elasticsearch.spark.sql").option("es.resource", indexName+"/movies")\
#     .option("es.nodes", "localhost").option("es.mapping.id", "id").mode("append").save()

movie_vectors.write.format("es") \
    .option("es.mapping.id", "id") \
    .option("es.write.operation", "update") \
    .save(indexName+"/movies", mode="append")

# write data to ES, use:
# - "id" as the column to map to ES movie id
# - "index" write mode for ES, since you have not written to the user index previously
# - "append" write mode for Spark
# user_vectors.write.format("org.elasticsearch.spark.sql").option("es.resource", indexName+"/movies")\
#     .option("es.nodes", "localhost").option("es.mapping.id", "id").mode("append").save()

user_vectors.write.format("es") \
    .option("es.mapping.id", "id") \
    .option("es.write.operation", "index") \
    .save(indexName+"/users", mode="append")

print(es.search(index=indexName, doc_type="movies", q="star wars phantom menace", size=1)['hits']['hits'][0])


