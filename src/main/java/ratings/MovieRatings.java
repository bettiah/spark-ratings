package ratings;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class MovieRatings {

    private static final String LOAD_PREFIX = "/Users/me/Downloads/";
    private static final String TITLE_RATINGS = "title.ratings.tsv.gz";
    private static final String TITLE_BASICS = "title.basics.tsv.gz";
    private static final String TITLE_AKAS = "title.akas.tsv.gz";
    private static final String TITLE_PRINCIPALS = "title.principals.tsv.gz";
    private static final String NAME_BASICS = "name.basics.tsv.gz";

    public static Dataset<Row> loadFile(SparkSession spark, String name) {
        return spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("delimiter", "\t")
                .option("nullValue", "\\N")
                .load(name);
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Movie Ratings").getOrCreate();

        Dataset<Row> ratings = loadFile(spark, LOAD_PREFIX + TITLE_RATINGS);
        Dataset<Row> titles = loadFile(spark, LOAD_PREFIX + TITLE_BASICS);

        // select & filter for movies
        Dataset<Row> movies = ratings
                .join(titles, "tconst")
                .select("tconst", "numVotes", "averageRating", "primaryTitle", "titleType")
                .where(functions.col("titleType").equalTo("movie"))
                .cache();
        movies.show();

        // 1:
        // Retrieve the top 10 movies with a minimum of 500 votes with the ranking determined by:
        // (numVotes/averageNumberOfVotes) * averageRating

        // get average number of votes for all movies
        double averageNumberOfVotes = movies
                .select(functions.mean("numVotes"))
                .collectAsList()
                .get(0)
                .getDouble(0);

        // rank and select top 10
        Dataset<Row> ranked = movies
                .where(functions.col("numVotes").gt(500))
                .withColumn("rank",
                        functions.col("numVotes")
                                .divide(averageNumberOfVotes)
                                .multiply(functions.col("averageRating")))
                .sort(functions.desc("rank"))
                .limit(10)
                .cache();

        ranked.show();

        ranked.select("primaryTitle").show();

        // 2.
        // For these 10 movies, list the persons who are most often credited
        // and
        // list the different titles of the 10 movies.


        // list the different titles of the 10 movies
        Dataset<Row> akas = loadFile(spark, LOAD_PREFIX + TITLE_AKAS).withColumnRenamed("titleId", "tconst");

        ranked
                .join(akas, "tconst")
                .groupBy("tconst", "rank", "primaryTitle")
                .agg(functions.collect_list("title").as("titles"))
                .sort(functions.desc("rank"))
                .select("primaryTitle", "titles")
                .show(false);

        // TODO - list the persons who are most often credited
        // do we need to fetch the people with most knownForTitles in names (max 4)
        // or
        // from most mentions for that movie ?
        Dataset<Row> names = loadFile(spark, LOAD_PREFIX + NAME_BASICS)
                .select(
                        functions.col("nconst"),
                        functions.col("primaryName"),
                        functions.explode(functions.split(functions.col("knownForTitles"), ",")).as("tconst")
                );

        names.groupBy("nconst")
                .agg(functions.count("tconst").as("count"))
                .show();

        ranked
                .join(names, "tconst")
                .select("primaryTitle", "primaryName", "nconst")
                .show();
//
//        Dataset<Row> credits = loadFile(spark, LOAD_PREFIX + TITLE_PRINCIPALS);
//        ranked
//                .join(credits, "tconst")
//                .groupBy("tconst")
//                .agg(functions.count("nconst").as("count"))
////                .sort(functions.desc("count"))
//                .show();


        spark.stop();
    }
}
