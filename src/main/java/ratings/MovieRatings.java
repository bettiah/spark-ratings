package ratings;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class MovieRatings {

    private static final String TITLE_RATINGS = "files/title.ratings.tsv.gz";
    private static final String TITLE_BASICS = "files/title.basics.tsv.gz";

    public static Dataset<Row> loadFile(SparkSession spark, String name) {
        return spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("delimiter", "\t")
                .load(name);
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

        // 1: Retrieve the top 10 movies with a minimum of 500 votes with the ranking determined by:
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
                .limit(10);

        ranked.show();

        ranked.select("primaryTitle").show();

        spark.stop();
    }
}
