package com.demo.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsTest {


	public static void main(String[] args) throws Exception {
		//readFile();
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://192.168.200.100:9000");
		FileSystem fileSystem = FileSystem.get(uri , conf);
		Path f = new Path("/user/hadoop/java-hdfs-test");
		FSDataOutputStream fsos = fileSystem.create(f);
		InputStream in = new FileInputStream(new File("C:/Users/Administrator/Desktop/Centos-6.repo"));
		IOUtils.copyBytes(in , fsos, 1024, true);
		IOUtils.closeStream(fsos);
	}

	private static void readFile() throws URISyntaxException, IOException {
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://192.168.200.100:9000");
		FileSystem fileSystem = FileSystem.get(uri , conf);
		FSDataInputStream openStream = fileSystem.open(new Path("/user/hadoop/opengp.log.2017-09-01.log"));
		IOUtils.copyBytes(openStream, System.out, 1024, false);
		IOUtils.closeStream(openStream);
	}
}
