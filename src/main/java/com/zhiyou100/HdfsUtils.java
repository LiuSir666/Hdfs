package com.zhiyou100;

import java.io.FileNotFoundException;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;

public class HdfsUtils {

	public static final Configuration CONF=new Configuration();
	
	public static FileSystem hdfs;
	static{
		try {
			hdfs=FileSystem.get(CONF);
		} catch (IOException e) {
			System.out.println("无法连接hdfs，请检查配置");
			e.printStackTrace();
		}
	}
	
	//在hdfs上创建一个新的文件，将某些数据写入到hdfs中
	private static void createFile(String fileName,String content) throws Exception {
		Path path=new Path(fileName);
		
		if (hdfs.exists(path)) {
			System.out.println("文件"+fileName+"在hdfs上已存在");
		} else {
			FSDataOutputStream outputStream=hdfs.create(path);
			outputStream.writeUTF(content);
			//outputStream.write(content.getBytes());
			outputStream.flush();
			outputStream.close();
		}

	}
	
	//读取hdfs上的已有文件
	public static void readFile(String fileName) throws IOException {
		Path path=new Path(fileName);
		if (!hdfs.exists(path)||hdfs.isDirectory(path)) {
			System.out.println("给定路径"+fileName+"不存在，或者不是一个文件");
		}else {
			FSDataInputStream inputStream=hdfs.open(path);
			String content=inputStream.readUTF();
			System.out.println(content);
		}
		
	}
	
	public static void deleteFile(String fileName) throws IOException {
		Path path=new Path(fileName);
		if (!hdfs.exists(path)) {
			System.out.println("给定的路径"+fileName+"不存在");
		}else {
			hdfs.delete(path, true);
		}
	}
	
	//把Windows本地的文件上传到hdfs上
	public static void uploadFile(String srcPath,String hdfsPath) throws IOException {
		Path src=new Path(srcPath);
		Path hdfspath=new Path(hdfsPath);
		
		hdfs.copyFromLocalFile(src, hdfspath);
	}
	
	public static void downloadFile(String hdfsPath,String localPath) throws IOException {
		Path hdfspath=new Path(hdfsPath);
		Path localpath=new Path(localPath);
		
		hdfs.copyToLocalFile(hdfspath, localpath);
	}
	
	public static void getFileStatus(String fileName) throws FileNotFoundException, IOException {
		Path path=new Path(fileName);
		FileStatus[] fileStatus=hdfs.listStatus(path);
		for (FileStatus fs : fileStatus) {
			System.out.println(fs);
		}
	}
	
	public static void getAllFileStatus(String fileName) throws IOException {
		Path path=new Path(fileName);
		if (hdfs.isDirectory(path)) {
			FileStatus[] fileStatus=hdfs.listStatus(path);
			for (FileStatus fs : fileStatus) {
				System.out.println(fs);
				getAllFileStatus(fs.getPath().toString());
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		String content="hello hdfs -刘Sir";
		String fileName="/dirFromJava";
		//createFile(fileName, content);
		
		//readFile(fileName);
		//deleteFile(fileName);
		
		//uploadFile("C:\\Users\\Administrator\\Desktop\\新建文本文档 (2).txt","/db17/");
		downloadFile("/db17/新建文本文档 (2).txt", "C:\\Users\\Administrator\\Desktop\\aaa.txt");
		//getFileStatus("/db17");
		//getAllFileStatus("/db17");
	}
}
