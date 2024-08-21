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
        configuration.set("fs.s3a.access.key", "AKIA54WIGLHMY64FBEHT");
        configuration.set("fs.s3a.path.style.access", "true");
        configuration.set("fs.s3a.connection.establish.timeout", "501000");
        configuration.set("spark.master", "local");
        configuration.set("fs.s3a.secret.key", "h5DaEqgvig5qLCkyC8AnpVqKG3LaOcxdn7C/T3ry");

        // Define schema
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false)
        });

        // Create a list of Rows
        List<Row> data = Arrays.asList(
                RowFactory.create("John Doe", 30),
                RowFactory.create("Jane Doe", 25)
        );

        // Create a DataFrame
        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        // Write the DataFrame to S3 in Parquet format
        df.write().parquet("s3a://sparks3aexample029/parquet_name_1/output.parquet");

        sparkSession.stop();
    }
}
