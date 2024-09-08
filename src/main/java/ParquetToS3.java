import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.RowFactory;

import java.util.Arrays;
import java.util.List;

public class ParquetToS3 {
    private static final String LOCAL_NODE_ID = "local[*]";
    private static final String APP_NAME = "sparks3aexample029";
    public static void main(String[] args) {
        SparkConf sparkConf =
                new SparkConf()
                        .set("spark.speculation", "false")
                        .set("spark.network.timeout", "600s")
                        .set("spark.executor.heartbeatInterval", "500s")
                        .setAppName(APP_NAME)
                        .setMaster(LOCAL_NODE_ID);
        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
        //sparkSession.sparkContext().setLogLevel("DEBUG");
        Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
        configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.s3a.path.style.access", "true");
        configuration.set("fs.s3a.connection.establish.timeout", "501000");
        configuration.set("spark.master", "local");
        configuration.set("fs.s3a.access.key", "${access.key}");
        configuration.set("fs.s3a.access.key", "${access.key}");

        // Step 2: Define schema
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false)
        });

        // Step 3: Create some sample data
        List<Row> data = Arrays.asList(
                RowFactory.create(1, "qwqwqwqw", 29),
                RowFactory.create(2, "test_two", 35),
                RowFactory.create(3, "test_three", 23)
        );

        // Step 4: Create DataFrame from data
        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        // Step 5: Write DataFrame to Parquet file in S3
        String outputPath = "s3a://sparks3aexample029/output/output.parquet";
        df.coalesce(1).write().mode("overwrite").parquet(outputPath);

        // Stop the Spark session
        sparkSession.stop();
    }
}
