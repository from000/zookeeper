package org.apache.zookeeper;

import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZookeeperMainTest {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 第一个参数： 服务器地址，一般为host:port,但是也可以是使用格式host:port/chroot
        // 第二个参数： session的超时时间
        // 第三个参数： watcher对象，在后面专门介绍
        ZooKeeper zookeeper = new ZooKeeper("localhost:2181/test", 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath());
                System.out.println(event.getState());
            }
        });

//
//        // -----------  同步创建节点   -----------
//        // 第一个参数： 创建节点路径path,如果指定了chroot，路径前需要加上chroot
//        // 第二个参数： 节点对应的字节数组
//        // 第三个参数： 节点权限，后面专门介绍
//        // 第四个参数： 创建的节点类型，主要有 ERSISTENT、PERSISTENT_SEQUENTIAL、EPHEMERAL和EPHEMERAL_SEQUENTIAL等
//        String path = zookeeper.create("/a", "b123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        // /a
//        System.out.println(path);
//
//        CountDownLatch latch = new CountDownLatch(1);
//
//        //-----------  异步创建节点   -----------
//        // 前四个参数同 同步创建节点 介绍
//        // 第五个参数： 处理异步结果
//        // 第六个参数： 传入异步处理的context对象，用于在异步创建节点结果返回时，做进一步处理
//        zookeeper.create("/a2","b223".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback(){
//            /**
//             *
//             * @param rc   服务端响应码：0 接口调用成功；-4 客户端和服务端连接断开；-110 节点已存在 ；-112 会话过期等
//             * @param path 创建节点指定的路径，对应zookeeper.create的第一个参数
//             * @param ctx  zookeeper.create的ctx参数
//             * @param name 服务端真正创建的路径，如果返回成功，一般对应的就是path
//             */
//            @Override
//            public void processResult(int rc, String path, Object ctx, String name) {
//                System.out.println(rc);
//                System.out.println(path);
//                System.out.println(ctx);
//                System.out.println(name);
//                latch.countDown();
//            }
//        },"context");
//        latch.await();

        //-----------  同步获取节点数据   -----------
        Stat stat = new Stat();
        // 第一个参数： 节点路径
        // 第二个参数： 是否需要watcher,如果设置为true,表示需要监听节点数据变化，当数据变化之后，可以在new Zookeeper的watcher对象中处理
        // 第三个参数： 节点状态
        byte[] data = zookeeper.getData("/a", true, stat);
        System.out.println(new String(data));
        System.out.println(stat);

    }
}
