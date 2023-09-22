package ratings;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class MovieRatingsTest extends SparkTest {

    @Test
    void testLoadFile() {
        Dataset<Row> rowDataset = MovieRatings.loadFile(spark, "src/test/resources/test.file.tsv");

        Assertions.assertEquals(1, rowDataset.count());

        List<Row> rows = rowDataset.collectAsList();
        Assertions.assertEquals("nm0000003", rows.get(0).getString(0));
        Assertions.assertNull(rows.get(0).getString(3));
    }
}