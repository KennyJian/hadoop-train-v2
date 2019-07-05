package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * 根据java操作hadoop的api
 * @Author: 黄思佳
 * @Date: 2019/7/5 16:27
 */
public class HDFSTest {

    public static final String HDFS_PATH = "hdfs://192.168.147.3:8020";
    Configuration configuration;
    FileSystem fileSystem;

    /**
     * 初始化配置
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception{
        System.out.println("--------start----------");
        configuration = new Configuration();

        //设置副本数，如果不设默认取3副本
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    @After
    public void tearDown() {

        System.out.println("-----------end------------");
    }

    /**
     * 创建文件夹
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {

        System.out.println(fileSystem.mkdirs(new Path("/hdfsapi/test")));
    }

    /**
     * 读文件
     * @throws IOException
     */
    @Test
    public void text() throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/README.txt"));
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
    }

    /**
     * 创建空文件并写入
     * @throws IOException
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/test/b.txt"));
        fsDataOutputStream.writeUTF("hello hadoop");
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    /**
     * 重命名文件
     * @throws IOException
     */
    @Test
    public void rename() throws IOException {

        System.out.println(fileSystem.rename(new Path("/hdfsapi/test/b.txt"), new Path("/hdfsapi/test/nb.txt")));
    }

    /**
     * 上传文件
     * @throws IOException
     */
    @Test
    public void copyFromLocalFile() throws IOException {

        fileSystem.copyFromLocalFile(new Path("D:/Notepad++/readme.txt"), new Path("/hdfsapi/test"));
    }

    /**
     * 带进度的上传文件
     * @throws IOException
     */
    @Test
    public void copyFromLocalFileWithProgress() throws IOException {

        InputStream inputStream = new BufferedInputStream(new FileInputStream(new File("E:/jdk-8u91-linux-x64.tar.gz")));

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/test/jdk.tgz"), new Progressable() {

            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(inputStream, fsDataOutputStream, 4096);
    }

    /**
     * 下载文件
     * @throws IOException
     */
    @Test
    public void copyToLocalFile() throws IOException {

        //坑：如果是在win系统上，不设置useRawLocalFileSystem=true的话，会导致下载文件失败
        fileSystem.copyToLocalFile(false, new Path("/hdfsapi/test/readme.txt"), new Path("E:"), true);
    }

    /**
     * 显示文件列表
     * @throws IOException
     */
    @Test
    public void listFile() throws IOException {

        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for (FileStatus fileStatus : fileStatuses){
            System.out.print(fileStatus.isDirectory() ? "文件夹" : "文件" + "\t");
            System.out.print(fileStatus.getPermission() + "\t");
            System.out.print(fileStatus.getReplication() + "\t");
            System.out.print(fileStatus.getOwner() + "\t");
            System.out.print(fileStatus.getGroup() + "\t");
            System.out.println(fileStatus.getPath());

        }
    }

    /**
     * 递归列出文件夹下的所有文件
     */
    @Test
    public void recursionFileList() throws IOException {

        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();

            System.out.print(fileStatus.isDirectory() ? "文件夹" : "文件" + "\t");
            System.out.print(fileStatus.getPermission() + "\t");
            System.out.print(fileStatus.getReplication() + "\t");
            System.out.print(fileStatus.getOwner() + "\t");
            System.out.print(fileStatus.getGroup() + "\t");
            System.out.println(fileStatus.getPath());
        }
    }

    /**
     * 获取文件块信息
     */
    @Test
    public void getFileBlockLocations() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/jdk.tgz"));

        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0 , fileStatus.getLen());

        for (BlockLocation blockLocation : blockLocations) {
            String[] cachedHosts = blockLocation.getNames();
            for (String cachedHost : cachedHosts) {
                System.out.println(cachedHost);
            }

        }
    }
}
