package cn.itcast.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperTest {
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		// ����һ������Zookeeper�ͻ���
		// ����1��zk��ַ������2���Ự��ʱʱ�䣨��ϵͳĬ��һ�£�������3��������
		ZooKeeper zk = new ZooKeeper("node-1:2181,node-2:2181,node-3:2181", 30000, new Watcher() {
			
			@Override
			// ������б��������¼���Ҳ��������������¼��Ĵ���
			public void process(WatchedEvent event) {
				System.out.println("�¼�����Ϊ��" + event.getType());
				System.out.println("�¼�������·����" + event.getPath());
				System.out.println("֪ͨ״̬Ϊ��" + event.getState());
			}
		});
		// �����������Ŀ¼�ڵ�
		// ����1��Ҫ�����Ľڵ��·��������2���ڵ����ݣ�����3���ڵ�Ȩ�ޣ�����4���ڵ�����
		zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		// ��������������Ŀ¼�ڵ�
		zk.create("/testRootPath/testChildPathOne", "testChildDataOne".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		// �����ģ���ȡĿ¼�ڵ�����
		// ����1���洢�ڵ����ݵ�·��������2���Ƿ���Ҫ��ش˽ڵ㣨true/false��������3��stat�ڵ��ͳ����Ϣ��һ������Ϊnull��
		System.out.println("testRootData�ڵ�����Ϊ��" + new String(zk.getData("/testRootPath", false, null)));
		// �����壺��ȡ��Ŀ¼�ڵ�����
		System.out.println(zk.getChildren("/testRootPath", true));
		// ���������޸���Ŀ¼�ڵ����ݣ�ʹ�ü�������
		// ����1���洢��Ŀ¼�ڵ����ݵ�·��������2��Ҫ�޸ĵ����ݣ�����3��Ԥ��Ҫƥ��İ汾������Ϊ-1�����ƥ���κνڵ�İ汾��
		zk.setData("/testRootPath/testChildPathOne", "modifyChildDataOne".getBytes(), -1);
		// �����ߣ��ж�Ŀ¼�ڵ��Ƿ����
		System.out.println("Ŀ¼�ڵ�״̬��[" + zk.exists("/testRootPath", true) + "]");
		// ����ˣ�ɾ����Ŀ¼�ڵ�
		zk.delete("/testRootPath/testChildPathOne", -1);
		// ����ţ�ɾ��Ŀ¼�ڵ�
		zk.delete("/testRootPath", -1);
		zk.close();
	}
}
