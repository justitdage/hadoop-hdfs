package com.kkb.hdfs.sequence;
 
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.IOUtils;
 import org.apache.hadoop.io.SequenceFile;
 import org.apache.hadoop.io.Writable;
 import org.apache.hadoop.util.ReflectionUtils;
 
 import java.io.IOException;
 
 public class SequenceFileReadNewVersion {
 
     public static void main(String[] args) throws IOException {
         //要读的SequenceFile
         String uri = "hdfs://node01:8020/writeSequenceFile";
         Configuration conf = new Configuration();
         Path path = new Path(uri);
 
         //Reader对象
         SequenceFile.Reader reader = null;
         try {
             //读取SequenceFile的Reader的路径选项
             SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(path);
 
             //实例化Reader对象
             reader = new SequenceFile.Reader(conf, pathOption);
 
             //根据反射，求出key类型
             Writable key = (Writable)
                     ReflectionUtils.newInstance(reader.getKeyClass(), conf);
             //根据反射，求出value类型
             Writable value = (Writable)
                     ReflectionUtils.newInstance(reader.getValueClass(), conf);
 
             long position = reader.getPosition();
             System.out.println(position);
 
             while (reader.next(key, value)) {
                 String syncSeen = reader.syncSeen() ? "*" : "";
                 System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                 position = reader.getPosition(); // beginning of next record
            }
        } finally {
             IOUtils.closeStream(reader);
        }
    }
 }

