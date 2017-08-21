package com.test;
import java.sql.Timestamp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.WriterOptions;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
public class ORCFile {
    public static void main(String[] args) throws Exception {
        // CREATE TABLE lxw_orc1 (
        // field1 STRING,
        // field2 STRING,
        // field3 STRING
        // ) stored AS orc;
        // TypeDescription schema =
        // TypeDescription.createStruct().addField("field1",
        // TypeDescription.createString())
        // .addField("field2",
        // TypeDescription.createDouble()).addField("field3",
        // TypeDescription.createLong());
        TypeDescription schema = TypeDescription.createStruct().addField("field1", TypeDescription.createLong())
                .addField("field2", TypeDescription.createDouble()).addField("field3", TypeDescription.createLong())
                .addField("field4", TypeDescription.createTimestamp()).addField("field5", TypeDescription.createString());
        String lxw_orc1_file = "/apps/ctgts/orc1file.orc";
        Configuration conf = new Configuration(); 
        WriterOptions opts=OrcFile.writerOptions(conf);
        opts.setSchema(schema);
        opts.stripeSize(67108864);//
        opts.bufferSize(131072);
        opts.blockSize(134217728);
        opts.compress(CompressionKind.ZLIB);
        opts.version(OrcFile.Version.V_0_12);
        FileSystem.getLocal(conf);
//        Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(schema).stripeSize(67108864).bufferSize(131072).blockSize(134217728)
//                .compress(CompressionKind.ZLIB).version(OrcFile.Version.V_0_12));
        Writer writer = OrcFile.createWriter(new Path(lxw_orc1_file), opts);
        Object[][] contents = new Object[][] { { 1000, 1.1, false, "2016-10-21 14:56:25", "abcd" }, { 2l, 1.2, true, "2016-10-22 14:56:25", "aaa" } };
        VectorizedRowBatch batch = schema.createRowBatch();
        for (Object[] content : contents) {
            int rowCount = batch.size++;
            System.out.println(content[0]);
            ((LongColumnVector) batch.cols[0]).vector[rowCount] = Long.parseLong(content[0].toString());
            System.out.println(content[1]);
            ((DoubleColumnVector) batch.cols[1]).vector[rowCount] = (Double) content[1];
            System.out.println(content[2]);
            ((LongColumnVector) batch.cols[2]).vector[rowCount] = content[2].equals(true) ? 1 : 0;
            System.out.println(content[3]);
            ((TimestampColumnVector) batch.cols[3]).time[rowCount] = (Timestamp.valueOf((String) content[3])).getTime();
            System.out.println(content[4]);
            ((BytesColumnVector) batch.cols[4]).setVal(rowCount, content[4].toString().getBytes("UTF-8"), 0, content[4].toString().length());
            // batch full
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size > 0) {
            writer.addRowBatch(batch);
        }
        writer.close();
        // String[] contents = new String[] { "1,a,aa", "2,b,bb", "3,c,cc",
        // "4,d,dd" };
        //
        // VectorizedRowBatch batch = schema.createRowBatch();
        // for (String content : contents) {
        // int rowCount = batch.size++;
        // String[] logs = content.split(",", -1);
        // for (int i = 0; i < logs.length; i++) {
        // // ((BytesColumnVector) batch.cols[i]).setVal(elementNum,
        // // sourceBuf, start, length);
        // System.out.println(logs[i]);
        // ((BytesColumnVector) batch.cols[i]).setVal(rowCount,
        // logs[i].getBytes("UTF8"), 0, logs[i].length());
        // // batch full
        // if (batch.size == batch.getMaxSize()) {
        // writer.addRowBatch(batch);
        // batch.reset();
        // }
        // }
        // }
        // writer.addRowBatch(batch);
        // writer.close();
        
    }
}
