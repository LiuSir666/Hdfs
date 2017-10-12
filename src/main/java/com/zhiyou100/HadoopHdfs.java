package com.zhiyou100;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopHdfs {

	//创建filesystem对象
	public static FileSystem getFileSystem() throws Exception{
		Configuration conf=new Configuration();
		FileSystem hdfs=FileSystem.get(conf);
		return hdfs;
	}
	
	//创建一个文件夹
	public static void mkdir(FileSystem hdfs,String dirName) throws IOException{
		Path path=new Path(dirName);
		if (hdfs.mkdirs(path)) {
			System.out.println("创建目录："+dirName+"成功");
		}else {
			System.out.println("创建目录："+dirName+"失败");
		}
	}
	
	public static void main(String[] args) throws Exception {
		FileSystem fileSystem=getFileSystem();
		mkdir(fileSystem, "/dirFromJava");
	}
}
