package ratings;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class SparkTest {

    protected SparkSession spark;

    @BeforeEach
    public void setUp() {
        spark = SparkSession.builder()
                .appName("Spark Test")
                .master("local[*]")
                .getOrCreate();
    }

    @AfterEach
    public void tearDown() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }
}
