package com.jd.es.example.common;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * Title: DeleteDoc 删除某个文档
 * Description: DeleteDoc
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class DeleteDoc {

    private static Logger logger = Logger.getLogger(DeleteDoc.class);

    public static void main(String[] args){
        try {
            //deleteDocWithId(Tool.INDEX_NAME, Tool.TYPE_NAME, "1");
            deleteDocWithId("transactions", "stock", "_search");
            //deleteDocWithId("date_test_index2", "my_type", "5");

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(),e);
        }
    }

    public static void deleteDocWithId(String indexName, String typeName, String docId) {
        DeleteResponse dResponse = Tool.CLIENT.prepareDelete(indexName, typeName, docId)
                .execute()
                .actionGet();
        String index = dResponse.getIndex();
        String type = dResponse.getType();
        String id = dResponse.getId();
        long version = dResponse.getVersion();
        System.out.println(index+","+type+","+id+","+version);
    }



    //批量删除文档
    public static void bulkDeleteDoc(String indexName, String typeName, String[] docIdArr){

        BulkRequestBuilder bulkRequestBuilder = Tool.CLIENT.prepareBulk();

        for (String docId : docIdArr) {
            bulkRequestBuilder.add(new DeleteRequest(indexName,typeName, docId));
        }
        BulkResponse responses = bulkRequestBuilder.get();
        logger.info(responses);
    }

}
