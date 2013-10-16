Simple example showing how to convert Avro files to Parquet in a serial fashion (i.e.
not using MapReduce).

Sample usage:

```bash
mvn compile
mvn exec:java -Dexec.mainClass="SerialAvro2Parquet" -Dexec.args="data/strings-100.avro target/strings-100.parquet"
```