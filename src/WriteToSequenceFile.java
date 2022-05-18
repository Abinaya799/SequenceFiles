/*
Hadoop is not a suitable environment for small dataset processing since it adds to lot of overhead to the namenode.

So it is advisable to convert the small datasets into a single sequence file

In this program you can create sequence file with or without compression.

The compression type is BLOCK

Instructions
1) Make sure you have included the native hadoop library as environment variable in .bashrc
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"
2) Make sure to add the following libraries
hadoop-common-2.4.0.jar
hadoop-hdfs-2.4.0.jar
hadoop-mapreduce-client-common-2.4.0.jar
hadoop-mapreduce-client-core-2.4.0.jar
*/


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/*To Throw exception if arguments are not sufficient
The arguments needed for the program is
1) Input dir with the small files
2) Output sequence file name
3) Boolean value: whether needs compression or not
*/
public class WriteToSequenceFile {


    public static class UserDefinedException extends Exception {
        UserDefinedException(String s) {
            super(s);
        }
    }

    public static void main(String[] args) throws UserDefinedException, IOException {

        if (!(args.length == 3)) {
            throw new UserDefinedException("3 arguments required namely the input file directory , output file " +
                    "directory and a boolean whether compression has to be performed or not");
        } else {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path input_path = new Path(args[0]);
            Path output_path = new Path(args[1]);
            LongWritable key = new LongWritable();
            Text value;
            value = new Text();
            SequenceFile.Writer writer;
            boolean compression = Boolean.parseBoolean(args[2]);

//            if compression is true then Block conversion is used and file type is GZip
            if (compression) writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(output_path),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new GzipCodec())
                    , SequenceFile.Writer.keyClass(key.getClass()), SequenceFile.Writer.valueClass(value.getClass()));
            else {
                writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(output_path),
                        SequenceFile.Writer.keyClass(key.getClass()), SequenceFile.Writer.valueClass(value.getClass()));
            }

//            Gets an array of list of files in the input path
            FileStatus[] status = fs.listStatus(input_path);
            int number_of_files = status.length;
            long key1 =0;
            for (int i = 0; i < number_of_files; i++) {

//                Gets the file according to index
                FileStatus currentFile = status[i];
                System.out.println("Processing file '" + currentFile.getPath().getName() + "'");

                // Stream the data from the file and read using Buffered Reader
                FSDataInputStream inputStream = fs.open(currentFile.getPath());
                BufferedReader readStream = new BufferedReader(new InputStreamReader(inputStream));

//                Write to the file
                while (inputStream.available() > 0) {
                    value.set(readStream.readLine());
                    key.set(key1++);
                    writer.append(key, value);

                }
                System.out.println("Written");
                inputStream.close();
            }
            fs.close();
            IOUtils.closeStream(writer);
            System.out.println("Sequence File Created!!");


        }
    }
}
