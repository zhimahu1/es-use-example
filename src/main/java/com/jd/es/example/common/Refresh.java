package com.jd.es.example.common;

import com.jd.es.example.query.CountDoc;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionResponse;

/**
 * Title: Refresh
 * Description: Refresh  在Elesticsearch中，写入打开一个新段的过程，叫做refresh。默认情况下，每个分片每秒自动刷新一次。
 * 这就是为什么说Elasticsearch是近实时的搜索了：文档的改动不会立即被搜索，但是会在一秒(可配)内可见。可以通过settings配置，"settings": {"refresh_interval": "1s" }
 * 这会困扰新用户：他们索引了个文档，尝试搜索它，但是搜不到。解决办法就是执行一次手动刷新，通过API:
    POST /_refresh    refresh所有索引
    POST /blogs/_refresh   只refresh 索引blogs
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/8/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class Refresh {

    private static Logger logger = Logger.getLogger(Refresh.class);

    /** 注意：
     * 虽然刷新比提交更轻量，但是它依然有消耗。人工刷新在测试写的时有用，但是不要在生产环境中每写一次就执行刷新，这会影响性能。
     * 相反，你的应用需要意识到ES近实时搜索的本质，并且容忍它。
     */

    /**
     * 不是所有的用户都需要每秒刷新一次。也许你使用ES索引百万日志文件，你更想要优化索引的速度，而不是进实时搜索。你可以通过修改配置项refresh_interval减少刷新的频率：1
     PUT /my_logs
         {
             "settings": {
             "refresh_interval": "30s" <1>
             }
         }
     */

    public static void main(String[] args) {

        String indexName = "test_refresh_index";
        String typeName = "ttt";


        long docSize1 = CountDoc.testCountDoc(indexName, typeName);
        IndexDoc.indexWithStr(indexName, typeName, "{}");//插入一个新文档
        long docSize2 = CountDoc.testCountDoc(indexName, typeName);
        logger.info("docSize1:" + docSize1 + " docSize2:" + docSize2 + " 因为没有刷新，docSize1 == docSize2");



        long docSize3 = CountDoc.testCountDoc(indexName, typeName);
        IndexDoc.indexWithStr(indexName, typeName, "{}");//插入一个新文档
        testRefresh(indexName);//刷新
        long docSize4 = CountDoc.testCountDoc(indexName, typeName);
        logger.info("docSize3:" + docSize3 + " docSize4:" + docSize4 + " 因为有刷新，docSize3 != docSize4 ");
    }

    //不推荐使用
    public static void testRefresh(String indexName){
        ActionResponse response = Tool.CLIENT.admin().indices().prepareRefresh(indexName).get();//Refresh 索引 indexName,不指定，刷新所以索引
    }
}