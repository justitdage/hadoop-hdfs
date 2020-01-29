package com.kkb;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class TestApi {

    /**
     * hdfs创建文件夹
     * @throws IOException
     */
    @Test
    public void mkdirToHDFS() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.mkdirs(new Path("/kaikeba/dir1"));
        fileSystem.close();
    }

    /**
     * 上传本地文件
     * @throws IOException
     */
    @Test
    public void uploadToHDFS() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.copyFromLocalFile(new Path("file:///c:\\Users\\Admin\\Desktop\\大数据课程前置环境准备.md"),new Path("hdfs://node01:8020/kaikeba/dir1"));
        fileSystem.close();
    }

    /**
     * 下载文件到本地
     * @throws IOException
     */
    @Test
    public void downloadToHDFS() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.copyToLocalFile(new Path("hdfs://node01:8020/kaikeba/dir1/大数据课程前置环境准备.md"),new Path("file:///d:"));
        fileSystem.close();
    }

    /**
     * 删除hdfs上的文件
     * @throws IOException
     */
    @Test
    public void deleteToHDFS() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path("hdfs://node01:8020/kaikeba/dir1/大数据课程前置环境准备.md"),true);
        fileSystem.close();
    }


    /**
     * 遍历文件
     * @throws IOException
     * @throws URISyntaxException
     */
    @Test
    public void listHDFS() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"),conf);
        //获取文件详情
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (remoteIterator.hasNext()){
            LocatedFileStatus status = remoteIterator.next();
            //文件名称
            System.out.println(status.getPath().getName());
            //文件长度
            System.out.println(status.getLen());
            //权限
            System.out.println(status.getPermission());
            //分组
            System.out.println(status.getGroup());

            //存储块的主机节点
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
        fileSystem.close();

    }

    /**
     * 用流的方式上传
     */
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"),conf);
        FileInputStream inputStream = new FileInputStream(new File("C:\\Users\\Admin\\Desktop\\1、基础环境及hdfs.docx"));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("hdfs://node01:8020/kaikeba/dir1/hdfs.docx"));
        //文件对拷贝
        IOUtils.copy(inputStream,fsDataOutputStream);
        fileSystem.close();
    }

    /**
     * 用流的方式下载
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    public void downLoadFileToHDFS() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"),conf);
        FSDataInputStream fis = fileSystem.open(new Path("hdfs://node01:8020/kaikeba/dir1/hdfs.docx"));
        FileOutputStream fos = new FileOutputStream(new File("C:\\Users\\Admin\\Desktop\\hdfs.docx"));

        //文件对拷贝
        IOUtils.copy(fis,fos);
        fileSystem.close();
    }
    /**
     * 小文件合并
     */
    @Test
    public void  mergeFile() throws URISyntaxException, IOException, InterruptedException {
        //获取分布式文件系统hdfs
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "hadoop");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("hdfs://node01:8020/bigfile.xml"));
        //获取本地文件系统 localFileSystem
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //读取本地的文件
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("file:///D:\\开课吧课程资料\\Hadoop&ZooKeeper课件\\最新版本课件\\hadoop与zookeeper课件资料\\1、第一天\\小文件合并"));
        for (FileStatus fileStatus : fileStatuses) {
            //获取每一个本地的文件路径
            Path path = fileStatus.getPath();
            //读取本地小文件
            FSDataInputStream fsDataInputStream = localFileSystem.open(path);
            IOUtils.copy(fsDataInputStream,fsDataOutputStream);
            IOUtils.closeQuietly(fsDataInputStream);
    }
        IOUtils.closeQuietly(fsDataOutputStream);
        localFileSystem.close();
        fileSystem.close();
        //读取所有本地小文件，写入到hdfs的大文件里面去
    }

}
