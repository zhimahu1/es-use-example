package com.jd.es.example.common;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;

/**
 * Title: Tool 工具类，提供基本配置以及ES客户端的创建和引用保存
 * Description: Tool
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class Tool {

    private static Logger logger = Logger.getLogger(Tool.class);

    //用集群名字，集群节点地址构建es client
    public static Client CLIENT = null;//保证单例，单例，单例！！！ 程序中千万不要new 多个client
    public static String CLUSTER_NAME = "jiesi-1";//每个集群都有一个固定的集群名，不是随便指定的，jiesi-1是测试环境的测试集群
    public static String INDEX_NAME = "agg_index";//索引，你可以指定自己的索引进行测试
    public static String TYPE_NAME = "agg_type";//类型，你可以指定自己的类型进行测试
    private static String CONNECTION_ADDRESS = "192.168.200.190:9303,192.168.200.191:9303";//要连接的ES节点的地址,端口是tcp端口
    public static String URL = "http://192.168.200.190:9203";//ES集群中某节点的ip,http端口，可以通过该地址使用http方式访问集群

    static {
        try {
            CLIENT = getClient(CLUSTER_NAME,CONNECTION_ADDRESS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    }

    /**
     * 创建ES客户端，请自己保证单例，单例，单例！！！千万不要new多个客户端
     * @param clusterName 集群名
     * @param ipPortList 支持多个ip，port,以逗号和分号分隔。格式 "ip:port,ip:port,ip:port"
     *                   输入示例 "192.168.200.190:9203,192.168.200.191:9203"
     * @return
     */
    public static synchronized Client getClient(String clusterName, String ipPortList){
        try{
            if(clusterName == null || clusterName.isEmpty() || ipPortList == null || ipPortList.isEmpty()){
                throw new Exception("集群名，连接地址没有设置");
            }

            //设置集群的名字
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", clusterName)
                    .put("client.transport.sniff", false)
                    .build();

            //创建集群client并添加集群节点地址
            //创建集群client并添加集群节点地址,可以addTransportAddress()多个ip和端口，增加连接的稳定性。
            TransportClient client = TransportClient.builder().settings(settings).build();
            String[] ipPortArr = ipPortList.split(",");//逗号分隔
            for (String ipPort : ipPortArr){
                String[] ip_port = ipPort.split(":");//冒号分隔
                if(ip_port.length == 2){
                    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip_port[0]), Integer.parseInt(ip_port[1])));
                }
            }
            return client;

        }catch (Exception e){
            logger.error(e.getMessage(),e);
            return null;
        }
    }
}
