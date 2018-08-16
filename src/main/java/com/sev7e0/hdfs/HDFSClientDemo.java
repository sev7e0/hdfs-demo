package com.sev7e0.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HDFSClientDemo {

	FileSystem fileSystem = null;

	@Before
	public void init() throws Exception {
		Configuration configuration = new Configuration();
//		configuration.set("fs.defaultFS","hdfs://spark01:9000" );
//		configuration.set("fs.file.impl","org.apache.hadoop.fs.LocalFileSystem" );
//		configuration.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem" );
		
		//可以在获取fileSystem对象时，可以配置uri 和用户名
		fileSystem = FileSystem.get(new URI("hdfs://spark01:9000"),configuration,"hadoopadmin");
	}

	@Test
	public void downloadCommod() throws IllegalArgumentException, IOException {
		//在文件拷贝与上传的过程中路径要精确到文件名
		fileSystem.copyToLocalFile(new Path("/eclipse/upload01"), new Path("/home/sev7e0/bigdata/hbase-2.0.1-src.tar.gz"));
		fileSystem.close();
	}
	
	@Test
	public void uploadCommond() throws Exception, IOException {
		fileSystem.copyFromLocalFile(new Path("/home/sev7e0/bigdata/hbase-2.0.1-src.tar.gz"),
				//没有精确到文件名，导致上传的文件名是upload01
				new Path("/eclipse/upload02/hbase-2.0.1-src.tar.gz"));
		fileSystem.close();
	}

}
