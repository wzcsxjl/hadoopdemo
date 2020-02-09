package com.itcast.hdfsdemo;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HDFS_CRUD {
	
	FileSystem fs = null;
	
	/**
	 * ��ʼ���ͻ��˶���
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception {
		// ����һ�����ò�����������һ��������Ҫ���ʵ�HDFS��URI
		Configuration conf = new Configuration();
		// ����ָ��ʹ�õ���HDFS
		conf.set("fs.defaultFS", "hdfs://node-1:9000");
		// ͨ�����µķ�ʽ���пͻ�����ݵ�����
		System.setProperty("HADOOP_USER_NAME", "root");
		// ͨ��FileSystem�ľ�̬������ȡ�ļ�ϵͳ�ͻ��˶���
		fs = FileSystem.get(conf);
	}
	
	/**
	 * �ϴ��ļ���HDFS
	 * @throws IOException
	 */
	@Test
	public void testAddFileToHdfs() throws IOException {
		// Ҫ�ϴ����ļ����ڱ���·��
		Path src = new Path("D:/test.txt");
		// Ҫ�ϴ���HDFS��Ŀ��·��
		Path dst = new Path("/testFile");
		// �ϴ��ļ�����
		fs.copyFromLocalFile(src, dst);
		// �ر���Դ
		fs.close();
	}
	
	/**
	 * ��HDFS�����ļ�������
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testDownloadFileToLocal() throws IllegalArgumentException, IOException {
		// �����ļ�
		fs.copyToLocalFile(new Path("/testFile"), new Path("D:/"));
		fs.close();
	}
	
	/**
	 * ������ɾ�����������ļ�
	 * @throws Exception
	 * @throws Exception
	 */
	@Test
	public void testMkdirAndDeleteAndRename() throws Exception, Exception {
		// ����Ŀ¼
		fs.mkdirs(new Path("/a/b/c"));
		fs.mkdirs(new Path("/a2/b2/c2"));
		// �������ļ����ļ���
		fs.rename(new Path("/a"), new Path("/a3"));
		// ɾ���ļ��У�����Ƿǿ��ļ��У�����2�����ֵtrue
		fs.delete(new Path("/a2"), true);
	}
	
	/**
	 * �鿴Ŀ¼��Ϣ��ֻ��ʾ�ļ�
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		// ��ȡ����������
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			// ��ӡ��ǰ�ļ���
			System.out.println(fileStatus.getPath().getName());
			// ��ӡ��ǰ�ļ����С
			System.out.println(fileStatus.getBlockSize());
			// ��ӡ��ǰ�ļ�Ȩ��
			System.out.println(fileStatus.getPermission());
			// ��ӡ��ǰ�ļ����ݳ���
			System.out.println(fileStatus.getLen());
			// ��ȡ���ļ�����Ϣ���������ȡ����ݿ顢datanode����Ϣ��
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				System.out.println("block-length: " + blockLocation.getLength() + "--" + "block-offset: " + blockLocation.getOffset());
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("-----------�ָ���-----------");
		}
	}

}
