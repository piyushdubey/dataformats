package net.piyushdubey.data;

import org.apache.iceberg.*;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class IcebergTableOperations {
    private static final String TABLE_LOCATION = "C:/Users/piyushdubey/source/repos/customer_data_" + System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("IcebergDemo")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .getOrCreate();

        // Create table schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "department", Types.StringType.get())
        );

        // Create table properties
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", "2");
        properties.put("write.delete.mode", "merge-on-read");
        properties.put("write.delete.vector.enabled", "true");
        properties.put("write.update.mode", "merge-on-read");

        // Initialize Hadoop Tables
        HadoopTables tables = new HadoopTables(spark.sparkContext().hadoopConfiguration());

        // Create table
        Table table = tables.create(schema, PartitionSpec.unpartitioned(), properties, TABLE_LOCATION);

        // Insert 20 records
        for (int i=1; i<=20; i++) {
            // Create a transaction for inserting records
            Transaction transaction = table.newTransaction();

            DataFile dataFile = insertRecord(transaction.table(), 1, table.io());

            transaction.newAppend()
                    .appendFile(dataFile)
                    .commit();
            transaction.commitTransaction();
        }

        System.out.println("Inserted 20 records successfully");

        // Delete 11 records (IDs 1-11)
        for (int id=1; id<=11; id++) {
            Transaction transaction = table.newTransaction();
            deleteRecord(transaction, id, table);
            transaction.commitTransaction();
        }

        System.out.println("Deleted 11 records successfully");

        // Print table metadata and statistics
        System.out.println("\nTable Statistics:");
        System.out.println("Total Records: " + table.currentSnapshot().summary().get("total-records"));
        System.out.println("Total Data Files: " + table.currentSnapshot().summary().get("total-data-files"));
        System.out.println("Total Delete Files: " + table.currentSnapshot().summary().get("total-delete-files"));
    }

    private static DataFile insertRecords(Table table, FileIO io) throws Exception {
        // Create a unique file path for the data file
        String filename = UUID.randomUUID().toString();
        OutputFile outputFile = io.newOutputFile(
                String.format("%s/data/%s.parquet", table.location(), filename));

        DataWriter<Record> dataWriter = Parquet.writeData(outputFile)
                .schema(table.schema())
                .withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .build();

        try {
            // Write 20 records
            for (int i = 1; i <= 20; i++) {
                Record record = GenericRecord.create(table.schema());
                record.setField("id", (long) i);
                record.setField("name", "Employee" + i);
                record.setField("department", "Department" + ((i % 4) + 1));
                dataWriter.write(record);
            }
        } finally {
            dataWriter.close();
        }

        // Create DataFile from written data
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(outputFile.toInputFile())
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    private static DataFile insertRecord(Table table, int id, FileIO io) throws Exception {
        // Create a unique file path for the data file
        String filename = UUID.randomUUID().toString();
        OutputFile outputFile = io.newOutputFile(
                String.format("%s/data/%s.parquet", table.location(), filename));

        DataWriter<Record> dataWriter = Parquet.writeData(outputFile)
                .schema(table.schema())
                .withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .build();

        try {
            Record record = GenericRecord.create(table.schema());
            record.setField("id", (long) id);
            record.setField("name", "Employee" + id);
            record.setField("department", "Department" + ((id % 4) + 1));
            dataWriter.write(record);
        } finally {
            dataWriter.close();
        }

        // Create DataFile from written data
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(outputFile.toInputFile())
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    private static void deleteRecords(Transaction transaction, Table table) {
        // Create delete file
        OutputFile deleteFile = table.io().newOutputFile(
                String.format("%s/delete/%s.parquet", table.location(), UUID.randomUUID()));

        DeleteFile deleteFileBuilder = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .withInputFile(deleteFile.toInputFile())
                .withFormat(FileFormat.PARQUET)
                .build();

        // Write delete file containing row positions to delete
        try (EqualityDeleteWriter<Record> deleteWriter = Parquet.writeDeletes(deleteFile)
                .forTable(table)
                .overwrite()
                .rowSchema(table.schema())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .buildEqualityWriter()) {

            // Delete records with IDs 1-11
            for (long id = 1; id <= 11; id++) {
                Record delete = GenericRecord.create(table.schema());
                delete.setField("id", id);
                deleteWriter.write(delete);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write delete file", e);
        }

        // Commit the delete file
        transaction.newRowDelta()
                .addDeletes(deleteFileBuilder)
                .commit();
    }

    private static void deleteRecord(Transaction transaction, int id, Table table) {
        // Create delete file
        OutputFile deleteFile = table.io().newOutputFile(
                String.format("%s/delete/%s.parquet", table.location(), UUID.randomUUID()));

        DeleteFile deleteFileBuilder = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .withInputFile(deleteFile.toInputFile())
                .withFormat(FileFormat.PARQUET)
                .build();

        // Write delete file containing row positions to delete
        try (EqualityDeleteWriter<Record> deleteWriter = Parquet.writeDeletes(deleteFile)
                .forTable(table)
                .overwrite()
                .rowSchema(table.schema())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .buildEqualityWriter()) {

            Record delete = GenericRecord.create(table.schema());
            delete.setField("id", id);
            deleteWriter.write(delete);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write delete file", e);
        }

        // Commit the delete file
        transaction.newRowDelta()
                .addDeletes(deleteFileBuilder)
                .commit();
    }
}