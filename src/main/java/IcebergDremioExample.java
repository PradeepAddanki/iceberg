import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class IcebergDremioExample {

    public static void main(String[] args) {
        // Step 1: Set up Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Iceberg Dremio Example")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                //.config("spark.sql.catalog.spark_catalog.warehouse", "hdfs://<your-hdfs-path>/warehouse")
                .getOrCreate();

        // Step 2: Load the Iceberg table
        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("namespace"), "table_name");
        Catalog catalog = new HadoopCatalog(spark.sparkContext().hadoopConfiguration(), "<your-hdfs-path>/warehouse");
        Table table = catalog.loadTable(tableIdentifier);

        // Step 3: Use Spark to read the Iceberg table
        Dataset<Row> df = spark.read().format("iceberg").load("<namespace>.<table_name>");

        // Step 4: Show the DataFrame contents
        df.show();

        // Step 5: Example Dremio JDBC connection (for more complex querying)
        String url = "jdbc:dremio:direct=<dremio_host>:<port>;user=<user>;password=<password>";
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM <namespace>.<table_name>");

            while (rs.next()) {
                System.out.println("Column1: " + rs.getString("column1") + ", Column2: " + rs.getString("column2"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Step 6: Stop the Spark Session
        spark.stop();
    }
}
