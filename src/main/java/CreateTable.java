import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.types.StructType;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CreateTable {
    public static void main(String[] args) throws Exception {

        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.accessKeyId", "${accessKeyId}");
        System.setProperty("aws.secretKey", "${secretKey}");
        System.setProperty("aws.secretAccessKey", "${secretAccessKey}");

        String filePath = "s3a://sparks3aexample029/output_3/path/output.parquet/part-00000-53747e91-eacd-45e0-bc37-588313c9c3f4-c000.snappy.parquet";
        String databaseName = "customers_databases";
        String tableName = "nyc_taxi_sv_10101";
        String storageFormat = "PARQUET";

        List<String> filePaths = new ArrayList<>();
        filePaths.add(filePath);


        Configuration conf = getAWSConfig();
        Path path = new Path(filePaths.get(0));


        FileSystem fs = path.getFileSystem(conf);
        long fileSize = fs.getFileStatus(path).getLen();

        HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
        ParquetMetadata metadata = ParquetFileReader.readFooter(inputFile, ParquetMetadataConverter.NO_FILTER);


        long rowCount = metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
        System.out.println("Row count: " + rowCount);

        Schema schema = parquetToIceSchema(path, conf);
        System.out.println("new Schema " + schema);



        PartitionSpec partitionspec = PartitionSpec.unpartitioned();

        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofMinutes(2)) // Set API call timeout
                .retryPolicy(RetryPolicy.builder().numRetries(5).build()) // Set retry policy
                .build();


        ImmutableList.Builder<DataFile> dataFilesBuilder = ImmutableList.builder();
        if (partitionspec.isUnpartitioned()) {
            // Handle this correctly later
            // Metrics metrics = loadMetrics(schema);
            DataFile dataFile = buildDataFile(filePath, partitionspec, storageFormat, fileSize, rowCount);
            dataFilesBuilder.add(dataFile);
        } else {
            // Metrics metrics = loadMetrics(schema);
            DataFile dataFile = buildDataFile(filePath, partitionspec, storageFormat, fileSize, rowCount);
            dataFilesBuilder.add(dataFile);
        }
        List<DataFile> dataFiles = dataFilesBuilder.build();

        Map<String, String> catalogProperties = new HashMap<>();

        catalogProperties.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
        catalogProperties.put("warehouse", AppConfig.WAREHOUSE_LOCATION);
        catalogProperties.put("aws.region", "us-east-1");
        // Replace with your S3 bucket
        catalogProperties.put("client", "glue");

        GlueCatalog catalog = new GlueCatalog();
        catalog.setConf(new Configuration());


        // If GlueCatalog required direct GlueClient, it should be managed here
        catalog.initialize("glue_catalog", catalogProperties);

        TableIdentifier identifier = TableIdentifier.of(databaseName, tableName);

        Transaction transaction = catalog.buildTable(identifier, schema)
                .withPartitionSpec(partitionspec)
                .withLocation(null)
                .createTransaction();


        Table table = transaction.table();
        newFileAppender.ensureNameMappingPresent(table);
        AppendFiles append = table.newAppend();
        dataFiles.forEach(append::appendFile);
        append.commit();
        transaction.commitTransaction();
        System.out.println("Successfully Committed Transactions");
    }


    private static Configuration getAWSConfig() {
        Configuration conf = new Configuration();
        // Use DefaultCredentialsProvider for AWS authentication
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        // Ensure fs.s3a.impl is set to use S3AFileSystem
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.connection.establish.timeout", "501000");
        conf.set("AWS_REGION", "us-east-2 ");
        return conf;
    }

    private static Schema parquetToIceSchema(Path path, Configuration conf) {
        try {
            HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
            ParquetMetadata metadata = ParquetFileReader.readFooter(inputFile, ParquetMetadataConverter.NO_FILTER);
            MessageType parquetSchema = metadata.getFileMetaData().getSchema();

            // Initialize the converter. Adjust the constructor parameters based on your requirements.
            boolean assumeBinaryIsString = true; // or false, depending on your data
            boolean assumeInt96IsTimestamp = true; // or false
            boolean caseSensitive = true; // or false
            boolean inferTimestampNTZ = false; // true if you want to infer TIMESTAMP_NTZ for int96 fields
            boolean nanosAsLong = false; // true if you are working with nanoseconds precision and want them as long

            ParquetToSparkSchemaConverter converter = new ParquetToSparkSchemaConverter(
                    assumeBinaryIsString,
                    assumeInt96IsTimestamp,
                    caseSensitive,
                    inferTimestampNTZ,
                    nanosAsLong
            );

            // Convert the MessageType (Parquet schema) to StructType (Spark schema)
            StructType sparkSchema = converter.convert(parquetSchema);

            // Print the Spark schema
            return SparkSchemaUtil.convert(sparkSchema);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Conversion from Parquet to Iceberg schema failed", e);
        }
    }

    private static DataFile buildDataFile(String filePath, PartitionSpec spec, String format, long fileSize, long recordCount) // , Metrics metrics)
    {
        return DataFiles.builder(spec)
                .withPath(filePath)
                .withFormat(format)
                .withFileSizeInBytes(fileSize)
                .withRecordCount(recordCount)
                // .withMetrics(metrics)
                // .withPartition(partition)
                .build();
    }
}
