import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.avro.AvroParquetWriter;

public class SerialAvro2Parquet {
  public static void main(String[] args) throws IOException {
    FileSystem fileSystem = FileSystem.getLocal(new Configuration());
    Path avroIn = new Path(args[0]);
    Path parquetOut = new Path(args[1]);

    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    DataFileStream<GenericRecord> stream = null;
    AvroParquetWriter<GenericRecord> parquetWriter = null;
    try {
      stream = new DataFileStream<GenericRecord>(fileSystem.open(avroIn), datumReader);
      Schema schema = stream.getSchema();
      parquetWriter = new AvroParquetWriter<GenericRecord>(parquetOut, schema);
      for (GenericRecord record : stream) {
        parquetWriter.write(record);
      }
    } finally {
      if (stream != null) {
        stream.close();
      }
      if (parquetWriter != null) {
        parquetWriter.close();
      }
    }
  }
}
