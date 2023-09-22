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
                .load(LOAD_PREFIX + name);
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local[4]")
                .appName("Movie Ratings").getOrCreate();

        Dataset<Row> ratings = loadFile(spark, TITLE_RATINGS);
        Dataset<Row> titles = loadFile(spark, TITLE_BASICS);

        // select & filter
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

//
//        Dataset<Row> credits = loadFile(spark, TITLE_PRINCIPALS);
//        ranked
//                .join(credits, "tconst")
//                .groupBy("tconst")
//                .agg(functions.count("nconst").as("count"))
////                .sort(functions.desc("count"))
//                .show();


        //
        Dataset<Row> names = loadFile(spark, NAME_BASICS)
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

        // list the different titles of the 10 movies
        Dataset<Row> akas = loadFile(spark, TITLE_AKAS).withColumnRenamed("titleId", "tconst");

        ranked
                .join(akas, "tconst")
                .groupBy("tconst", "rank", "primaryTitle")
                .agg(functions.collect_list("title").as("titles"))
                .sort(functions.desc("rank"))
                .select("primaryTitle", "titles")
                .show(false);

        spark.stop();
    }
}
