package net.piyushdubey.data;

import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class App {
    public static void main(String[] args) {
        
        String tableLocation = "/Users/piyushdubey/projects/icebergtables/customer_data";

        // Create table
        Table table = createIcebergTableWithoutCatalog(tableLocation);

        // Or load existing table
        // Table table = loadExistingTable(tableLocation);

        // Delete records which will automatically generate deletion vectors
        deleteRecords(table);

        // Show current snapshot information
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot != null) {
            System.out.println("Current snapshot ID: " + snapshot.snapshotId());
            System.out.println("Timestamp: " + snapshot.timestampMillis());
            System.out.println("Operation: " + snapshot.operation());
            System.out.println("Manifest list location: " + snapshot.manifestListLocation());
        }
    }

    public static void insertRecords(Table table) {
        try {
            List<Record> records = new ArrayList<>();
            
            // Create sample records
            records.add(createRecord(table.schema(), 1L, "John Doe", 30, 
                LocalDateTime.of(2024, 1, 15, 10, 30)));
            records.add(createRecord(table.schema(), 2L, "Jane Smith", 25, 
                LocalDateTime.of(2024, 1, 15, 14, 45)));
            records.add(createRecord(table.schema(), 3L, "Bob Johnson", 45, 
                LocalDateTime.of(2024, 1, 16, 9, 15)));
            records.add(createRecord(table.schema(), 4L, "Alice Brown", 35, 
                LocalDateTime.of(2024, 1, 16, 16, 20)));
            records.add(createRecord(table.schema(), 5L, "Charlie Wilson", 50, 
                LocalDateTime.of(2024, 1, 17, 11, 30)));

            // Create output file
            OutputFile outputFile = table.io().newOutputFile(
                table.locationProvider().newDataLocation(table.spec(), table.locationProvider().newDataPath())
            );

            // Create file appender for Parquet
            try (FileAppender<Record> appender = Parquet.write(outputFile)
                    .schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build()) {
                for (Record record : records) {
                    appender.add(record);
                }
            }

            // Write records
            try (FileAppender<Record> writer = appender) {
                for (Record record : records) {
                    writer.add(record);
                }
            }

            // Create DataFile from written file
            DataFile dataFile = DataFiles.builder(table.spec())
                .withInputFile(outputFile.toInputFile())
                .withMetrics(appender.metrics())
                .withFormat(FileFormat.PARQUET)
                .build();

            // Commit the changes
            table.newAppend()
                .appendFile(dataFile)
                .commit();

            System.out.println("Successfully inserted " + records.size() + " records");
            
        } catch (Exception e) {
            System.err.println("Error inserting records: " + e.getMessage());
            e.printStackTrace();
        }
    }        

        private static Record createRecord(Schema schema, Long id, String name, Integer age, LocalDateTime timestamp) {
            Record record = GenericRecord.create(schema);
            record.setField("id", id);
            record.setField("name", name);
            record.setField("age", age);
            record.setField("timestamp", timestamp);
            return record;
        }

        public static void deleteRecords(Table table) {
        try {
            // Start a new transaction
            Transaction tx = table.newTransaction();

            // Delete records with specific conditions
            tx.newDelete()
                .set("where", "id IN (1, 2, 5)")  // Delete records with specific IDs
                .commit();

            // Delete records with age > 60
            tx.newDelete()
                .deleteFromRowFilter(Expressions.greaterThan("age", 60))
                .commit();

            // Delete records with a specific name
            tx.newDelete()
                .deleteFromRowFilter(Expressions.equal("name", "John Doe"))
                .commit();

            // Delete records within a specific time range
            tx.newDelete()
                .deleteFromRowFilter(Expressions.and(
                    Expressions.greaterThanOrEqual("timestamp", "2024-01-01T00:00:00"),
                    Expressions.lessThan("timestamp", "2024-02-01T00:00:00")))
                .commit();

            // Commit all the deletes in one transaction
            tx.commitTransaction();
            
            System.out.println("Successfully deleted records. Deletion vectors will be automatically generated.");
            
        } catch (Exception e) {
            System.err.println("Error deleting records: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static Table createIcebergTableWithoutCatalog(String tableLocation) {
        try {
            // Initialize HadoopTables
            Configuration conf = new Configuration();
            HadoopTables tables = new HadoopTables(conf);

            // Define schema
            Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "age", Types.IntegerType.get()),
                Types.NestedField.required(4, "timestamp", Types.TimestampType.withoutZone())
            );

            // Define partitioning
            PartitionSpec spec = PartitionSpec.builderFor(schema)
                .month("timestamp")
                .build();

            // Create table properties
            Map<String, String> properties = new HashMap<>();
            properties.put("write.format.default", "parquet");
            properties.put("write.parquet.compression-codec", "snappy");
            properties.put("format-version", "2");
            properties.put("write.delete.mode", "merge-on-read");

            // Create table
            Table table = tables.create(
                schema,
                spec,
                properties,
                tableLocation
            );
            
            System.out.println("Successfully created Iceberg table at: " + tableLocation);
            return table;
            
        } catch (Exception e) {
            System.err.println("Error creating Iceberg table: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to create table", e);
        }
    }

    public static Table loadExistingTable(String tableLocation) {
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        return tables.load(tableLocation);
    }
}