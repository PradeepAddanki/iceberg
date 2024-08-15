import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkS3AExample {

    private static final String LOCAL_NODE_ID = "local[*]";
    private static final String FORMAT = "csv";
    private static final String APP_NAME = "SparkS3AExample";


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
       // sparkSession.sparkContext().setLogLevel("DEBUG");
        Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
        configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.s3a.access.key", "<ACCESS_KEY>");
        configuration.set("fs.s3a.path.style.access", "true");
        configuration.set("fs.s3a.connection.establish.timeout", "501000");
        configuration.set("spark.master", "local");
        configuration.set("fs.s3a.secret.key", "<SECRET_KEY>");
        configuration.set("fs.s3a.endpoint", "<S3A_ENDPOINT>");
        // Read a CSV file with the header, and store it in a DataFrame.
        Dataset<Row> df = sparkSession.read().format(FORMAT)
                .option("header", "true")
                .load("s3a://ramirtt/names.csv");
        //Show the first 15 rows.
        df.show(10);
    }

}