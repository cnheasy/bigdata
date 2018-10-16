package com.heasy.api;

import com.jcraft.jsch.IO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class ApiDemo {
    /**
     * 上传文件
     *
     * @throws IOException
     */
    @Test
    public void getFileSystemUpload() throws IOException, URISyntaxException, InterruptedException {
        //设置环境变量
        FileSystem fileSystem = getFileSystem();
        System.out.println(fileSystem);
        fileSystem.copyFromLocalFile(new Path("/Users/zhiyang/Desktop/welcome.html"), new Path("/user/heasy"));
    }

    /**
     * 通过流上传文件
     *
     * @throws IOException
     */
    @Test
    public void getFileSystemUploadStream() throws IOException, URISyntaxException, InterruptedException {
        //设置环境变量
        FileSystem fileSystem = getFileSystem();
//获取输入流
        FileInputStream fileInputStream = new FileInputStream("/Users/zhiyang/Desktop/05_二次排序bean对象.avi");
        //获取输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/user/heasy/test.avi"));
//拷贝流
        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 4096);
        //关闭流
        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fsDataOutputStream);


    }


    /**
     * 修改文件名称
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void rename() throws IOException, URISyntaxException, InterruptedException {
        //设置环境变量
        FileSystem fileSystem = getFileSystem();
        System.out.println(fileSystem);
        boolean rename = fileSystem.rename(new Path("/user/heasy/welcome.html"), new Path("/user/heasy/welcome1.html"));
        System.out.println(rename);
    }


    /**
     * 创建文件夹文件
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void createDir() throws IOException, URISyntaxException, InterruptedException {
        //设置环境变量
        FileSystem fileSystem = getFileSystem();
        System.out.println(fileSystem);
        boolean mkdirs = fileSystem.mkdirs(new Path("/user/heasy/"));
        fileSystem.create(new Path("/user/heasy/test.txt"));
    }

    /**
     * 下载
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void download() throws IOException, URISyntaxException, InterruptedException {
        //设置环境变量
        FileSystem fileSystem = getFileSystem();
        System.out.println(fileSystem);
        fileSystem.copyToLocalFile(new Path("/user/heasy/test.avi"), new Path("/Users/zhiyang/Desktop/test.html"));
    }
    /**
     * 通过流下载
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void downloadStream() throws IOException, URISyntaxException, InterruptedException {
        //设置环境变量
        FileSystem fileSystem = getFileSystem();
        //输入流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/user/heasy/address.js"));
        //输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/Users/zhiyang/Desktop/add.js"));
        //拷贝
        IOUtils.copyBytes(fsDataInputStream,fileOutputStream,4096);
        IOUtils.closeStream(fsDataInputStream);
        IOUtils.closeStream(fileOutputStream);
    }

    /**
     * 删除文件
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void delete() throws IOException, URISyntaxException, InterruptedException {
        //设置环境变量
        FileSystem fileSystem = getFileSystem();
        System.out.println(fileSystem);
        fileSystem.delete(new Path("/user/heasy"), true);
    }


    /**
     * 获取文件目录
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void getFileDir() throws IOException, URISyntaxException, InterruptedException {
        FileSystem fileSystem = getFileSystem();

        System.out.println(fileSystem);
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/user/heasy/"));
        for (FileStatus file : fileStatuses) {
            if (file.isDirectory()) {
                System.out.println(file + "  is dir");
            } else {
                System.out.println(file + " is file");
            }
            System.out.println("accessTime: " + file.getAccessTime());
            System.out.println("blockSize: " + file.getBlockSize());
            System.out.println("group: " + file.getGroup());
            System.out.println("user: " + file.getOwner());
            System.out.println("len: " + file.getLen());
            FsPermission permission = file.getPermission();
//            permission.
            System.out.println("replication: " + file.getReplication());
        }

    }

    /**
     * 获取文件系统
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    private FileSystem getFileSystem() throws IOException, InterruptedException, URISyntaxException {
        //设置环境变量
        System.setProperty("hadoop.home.dir", "/Volumes/heasy/02_大数据技术之Hadoop/02_大数据技术之Hadoop/2.资料/01_jar包/hadoop-2.7.2");

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://hadoop14:9000");
        return FileSystem.get(new URI("hdfs://hadoop14:9000"), conf, "heasy");
    }


}
