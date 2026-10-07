from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

# ---------------------------------------------------------------------------
# CONFIG - tune these to experiment with the results (Improvement #3)
# ---------------------------------------------------------------------------
MIN_RATING_TO_KEEP = 4          # Improvement #1: discard ratings below this
SIMILARITY_METRIC = "pearson"   # Improvement #2: "cosine" or "pearson"
MIN_CO_RATERS = 10              # Improvement #4: dampening strength
GENRE_BOOST = 0.05              # Improvement #5: bonus per shared genre
SCORE_THRESHOLD = 0.90
CO_OCCURRENCE_THRESHOLD = 10.0


# ---------------------------------------------------------------------------
# Improvement #2a: original cosine similarity
# ---------------------------------------------------------------------------
def computeCosineSimilarity(data):
    pairScores = data \
        .withColumn("xx", func.col("rating1") * func.col("rating1")) \
        .withColumn("yy", func.col("rating2") * func.col("rating2")) \
        .withColumn("xy", func.col("rating1") * func.col("rating2"))

    calculateSimilarity = pairScores \
        .groupBy("movie1", "movie2") \
        .agg(
            func.sum(func.col("xy")).alias("numerator"),
            (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"),
            func.count(func.col("xy")).alias("numPairs")
        )

    result = calculateSimilarity \
        .withColumn("score",
            func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator"))
                .otherwise(0)
        ).select("movie1", "movie2", "score", "numPairs")

    return result


# ---------------------------------------------------------------------------
# Improvement #2b: Pearson correlation coefficient
# Corrects for a user's overall rating bias (e.g. someone who rates
# everything 5 stars) which cosine similarity is sensitive to.
# ---------------------------------------------------------------------------
def computePearsonCorrelation(data):
    stats = data \
        .groupBy("movie1", "movie2") \
        .agg(
            func.count(func.col("rating1")).alias("n"),
            func.sum(func.col("rating1")).alias("sumX"),
            func.sum(func.col("rating2")).alias("sumY"),
            func.sum(func.col("rating1") * func.col("rating1")).alias("sumXX"),
            func.sum(func.col("rating2") * func.col("rating2")).alias("sumYY"),
            func.sum(func.col("rating1") * func.col("rating2")).alias("sumXY")
        )

    numerator = (func.col("n") * func.col("sumXY")) - (func.col("sumX") * func.col("sumY"))
    denominator = func.sqrt(
        (func.col("n") * func.col("sumXX") - func.col("sumX") * func.col("sumX")) *
        (func.col("n") * func.col("sumYY") - func.col("sumY") * func.col("sumY"))
    )

    result = stats \
        .withColumn("score",
            func.when(denominator != 0, numerator / denominator).otherwise(0)
        ) \
        .select("movie1", "movie2", "score", func.col("n").alias("numPairs"))

    return result


# ---------------------------------------------------------------------------
# Improvement #4: weight/dampen the raw similarity score by how many
# co-raters back it up. A high score from only 3 shared raters is less
# trustworthy than a slightly lower score backed by 200 shared raters.
# confidence -> 1 as numPairs grows, and shrinks toward 0 for small numPairs.
# ---------------------------------------------------------------------------
def applyCoRaterWeighting(similarities, minCoRaters=MIN_CO_RATERS):
    confidence = func.col("numPairs") / (func.col("numPairs") + func.lit(minCoRaters))
    return similarities.withColumn("score", func.col("score") * confidence)


# ---------------------------------------------------------------------------
# Improvement #5: boost scores for movie pairs that share genres.
# u.item has 19 trailing binary genre flags (fields 5..23, 0-indexed).
# ---------------------------------------------------------------------------
def loadGenreVectors(spark, moviesPath):
    genreCols = [f"genre{i}" for i in range(19)]
    movieDetailSchema = StructType(
        [StructField("movieID", IntegerType(), True),
         StructField("movieTitle", StringType(), True),
         StructField("releaseDate", StringType(), True),
         StructField("videoReleaseDate", StringType(), True),
         StructField("imdbUrl", StringType(), True)] +
        [StructField(c, IntegerType(), True) for c in genreCols]
    )

    movieDetails = spark.read \
        .option("sep", "|") \
        .option("charset", "ISO-8859-1") \
        .schema(movieDetailSchema) \
        .csv(moviesPath)

    genreVectors = movieDetails.select(
        "movieID", func.array(*[func.col(c) for c in genreCols]).alias("genres")
    )
    return genreVectors


def applyGenreBoost(similarities, genreVectors, boost=GENRE_BOOST):
    g1 = genreVectors.withColumnRenamed("movieID", "movie1").withColumnRenamed("genres", "genres1")
    g2 = genreVectors.withColumnRenamed("movieID", "movie2").withColumnRenamed("genres", "genres2")

    joined = similarities.join(g1, "movie1").join(g2, "movie2")

    # Count how many genre flags match (both are 1) between the two movies.
    sharedGenres = func.aggregate(
        func.zip_with("genres1", "genres2", lambda a, b: (a * b).cast("int")),
        func.lit(0), lambda acc, x: acc + x
    )

    return joined.withColumn(
        "score",
        func.col("score") + (sharedGenres.cast("double") * func.lit(boost))
    ).select("movie1", "movie2", "score", "numPairs")


# Get movie name by given movie id
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]
    return result[0]


spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([
    StructField("movieID", IntegerType(), True),
    StructField("movieTitle", StringType(), True)
])

moviesSchema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Broadcast dataset of movieID -> movieTitle for lookups
movieNames = spark.read \
    .option("sep", "|") \
    .option("charset", "ISO-8859-1") \
    .schema(movieNamesSchema) \
    .csv("C:/SparkCourse/ml-100k/u.item")

# Genre vectors for the boost step (Improvement #5)
genreVectors = loadGenreVectors(spark, "C:/SparkCourse/ml-100k/u.item")

# Load ratings
movies = spark.read \
    .option("sep", "\t") \
    .schema(moviesSchema) \
    .csv("C:/SparkCourse/ml-100k/u.data")

# ---------------------------------------------------------------------------
# Improvement #1: discard bad ratings before doing anything else.
# Only "good" ratings count toward a co-occurrence, so two movies are
# only considered similar based on users who *liked* both of them.
# ---------------------------------------------------------------------------
ratings = movies.select("userId", "movieId", "rating") \
    .filter(func.col("rating") >= MIN_RATING_TO_KEEP)

# Self-join to find every movie pair rated by the same user
moviePairs = ratings.alias("ratings1") \
    .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId"))
          & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
    .select(func.col("ratings1.movieId").alias("movie1"),
            func.col("ratings2.movieId").alias("movie2"),
            func.col("ratings1.rating").alias("rating1"),
            func.col("ratings2.rating").alias("rating2"))

# --- Compute base similarity (Improvement #2) ---
if SIMILARITY_METRIC == "pearson":
    moviePairSimilarities = computePearsonCorrelation(moviePairs)
else:
    moviePairSimilarities = computeCosineSimilarity(moviePairs)

# --- Apply co-rater confidence weighting (Improvement #4) ---
moviePairSimilarities = applyCoRaterWeighting(moviePairSimilarities)

# --- Apply genre boost (Improvement #5) ---
moviePairSimilarities = applyGenreBoost(moviePairSimilarities, genreVectors)

moviePairSimilarities = moviePairSimilarities.cache()

if len(sys.argv) > 1:
    movieID = int(sys.argv[1])

    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &
        (func.col("score") > SCORE_THRESHOLD) & (func.col("numPairs") > CO_OCCURRENCE_THRESHOLD))

    results = filteredResults.sort(func.col("score").desc()).take(10)

    print("Top 10 similar movies for " + getMovieName(movieNames, movieID))

    for result in results:
        similarMovieID = result.movie1
        if similarMovieID == movieID:
            similarMovieID = result.movie2

        print(getMovieName(movieNames, similarMovieID) + "\tscore: "
              + str(result.score) + "\tstrength: " + str(result.numPairs))