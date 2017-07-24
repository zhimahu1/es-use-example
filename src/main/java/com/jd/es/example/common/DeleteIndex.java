package com.jd.es.example.common;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;

import java.util.concurrent.ExecutionException;

/**
 * Title: DeleteIndex 为了集群安全，稳定，不能删索引，要删除索引请在esm.jd.com上提交申请
 * Description: DeleteIndex      就像不要自己用SQL删数据库表一样，不要在代码中自作主张删索引，后果很严重啊！
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/22
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
@Deprecated
public class DeleteIndex {

    private static Logger logger = Logger.getLogger(DeleteIndex.class);

    public static void main(String[] args){
        testDeleteIndex("cc20160823");
    }

    /**《再三强调》
     删索引，
     风险大，
     容易把，
     集群搞挂！

     线上环境，
     不要自己删索引，
     要删索引，
     请管理端提申请！
     */
    @Deprecated
    public static  void testDeleteIndex(String indexName){
        try {
            DeleteIndexRequestBuilder deleteIndexRequestBuilder = Tool.CLIENT.admin().indices().prepareDelete(indexName);
            deleteIndexRequestBuilder.get();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(),e);
        }
    }
}
