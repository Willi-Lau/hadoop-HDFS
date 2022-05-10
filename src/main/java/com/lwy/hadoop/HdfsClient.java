package com.lwy.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws Exception {
        //连接集群的地址
        URI uri = new URI("hdfs://hadoop102:8020");
        //配置文件
        Configuration configuration = new Configuration();
        //连接信息
        fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "root");
    }

    @After
    public void close() throws Exception {
        fs.close();
    }

    /**
     * 测试创建文件夹
     * @throws Exception
     */
    @Test
    public void testMkdirs() throws Exception {
        // 执行命令
        fs.mkdirs(new Path("/xiyou/huaguoshan2"));
    }
    /**
     * 测试上传
     */
    @Test
    public void testPut() throws Exception{
        //参数列表 1 是否删除原数据 2 是否覆盖 3 源文件路径 4 目的地
        fs.copyFromLocalFile(false,true,new Path("C:\\Users\\Administrator\\Desktop\\ebay.txt"),new Path("/xiyou/huaguoshan2"));
    }

    /**
     * 测试下载
     */
    @Test
    public void testGet() throws Exception{
        //参数解读 1 原文件是否删除 2 HDFS路径 3 目标地址路径 windows 4 是否开启校验
        fs.copyToLocalFile(false,new Path("/xiyou/huaguoshan2"),new Path("C:\\Users\\Administrator\\Desktop\\hadoopTest"),false);
    }

    /**
     * 文件删除
     */
    @Test
    public void testDel() throws Exception{
        //参数解读 1 要删除的HDFS路径 2 是否递归删除
        fs.delete(new Path("/xiyou/huaguoshan2/ebay.txt"),false);

        //删除空目录
        fs.delete(new Path("/xiyou/huaguoshan3"),false);
        //删除非空目录 需要递归删除
        fs.delete(new Path("/xiyou/huaguoshan3"),true);
    }

    /**
     * 文件更名和移动
     */
    @Test
    public void testMove() throws IOException{
        //参数解读 1 原文件路径 目标文件路径
        fs.rename(new Path("/xiyou/huaguoshan2/ebay.txt"),new Path("/xiyou/huaguoshan2/ebay2.txt"));

        //文件移动和更名
        fs.rename(new Path("/xiyou/huaguoshan2/ebay2.txt"),new Path("/xiyou/huaguoshan3/ebay2.txt"));
    }

    /**
     * 获取文件详细信息1
     */
    @Test
    public void fileDetail() throws IOException{
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
            System.out.println(locatedFileStatus);

        }
    }


    /**
     * 判断是文件夹还是文件  123123
     */
    @Test
    public void testFileType() throws IOException{
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses){
            System.out.println(fileStatus.getPath().getName() + "  " + (fileStatus.isFile()?"文件":"文件夹"));
        }
    }


}
