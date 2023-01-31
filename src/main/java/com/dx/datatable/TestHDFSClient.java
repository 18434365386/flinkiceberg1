package com.dx.datatable;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;

public class TestHDFSClient {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //这里指定使用的是 hdfs文件系统
        conf.set("fs.defaultFS", "hdfs://192.168.6.102:9000");

        //通过这种方式设置java客户端身份
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fs = FileSystem.get(conf);
        //或者使用下面的方式设置客户端身份
        //FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"root");

        // fs.create(new Path("/helloByJava")); //创建一个目录

        //文件下载到本地 如果出现0644错误或找不到winutils.exe,则需要设置windows环境和相关文件.
        //fs.copyToLocalFile(new Path("/zookeeper.out"), new Path("D:\\test\\examplehdfs"));


        //使用Stream的形式操作HDFS，这是更底层的方式
        FSDataOutputStream outputStream = fs.create(new Path("/2.txt"), true); //输出流到HDFS
        FileInputStream inputStream = new FileInputStream("D:/test/examplehdfs/1.txt"); //从本地输入流。
        IOUtils.copy(inputStream, outputStream); //完成从本地上传文件到hdfs

        fs.close();
    }
}