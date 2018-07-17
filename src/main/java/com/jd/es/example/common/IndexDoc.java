package com.jd.es.example.common;


import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;

/**
 * Title: IndexDoc
 * Description: IndexDoc
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class IndexDoc {

    private static Logger logger = Logger.getLogger(IndexDoc.class);

    public static void main(String[] args){

        prepareTestDoc();
    }

    //准备以供测试的文档，往agg_index 写入8个文档
    public static void prepareTestDoc(){

        String json1 = "{" +
                "\"depCity\":\"北京\"," +
                "\"arrCity\":\"上海\"," +
                "\"queryDate\":\"2018-06-21 00:00:00\"," +
                "\"userPin\":\"huyanxia\"" +
                "}";

        String json2 = "{" +
                "\"depCity\":\"北京\"," +
                "\"arrCity\":\"三亚\"," +
                "\"queryDate\":\"2018-06-21 00:00:00\"," +
                "\"userPin\":\"huyanxia1\"" +
                "}";

        String json3 = "{" +
                "\"depCity\":\"南京\"," +
                "\"arrCity\":\"北京\"," +
                "\"queryDate\":\"2018-06-24 00:00:00\"," +
                "\"userPin\":\"huyanxia2\"" +
                "}";

        String json4 = "{" +
                "\"depCity\":\"西安\"," +
                "\"arrCity\":\"延安\"," +
                "\"queryDate\":\"2018-06-22 00:00:00\"," +
                "\"userPin\":\"huyanxia\"" +
                "}";

        String json5 = "{" +
                "\"depCity\":\"海南\"," +
                "\"arrCity\":\"山东\"," +
                "\"queryDate\":\"2018-06-22 00:00:00\"," +
                "\"userPin\":\"huyanxia\"" +
                "}";

        String json6 = "{" +
                "\"depCity\":\"上\"," +
                "\"arrCity\":\"北京\"," +
                "\"queryDate\":\"2018-06-23 00:00:00\"," +
                "\"userPin\":\"huyanxia\"" +
                "}";

        String json7 = "{" +
                "\"depCity\":\"上海\"," +
                "\"arrCity\":\"北京\"," +
                "\"queryDate\":\"2018-06-23 00:00:00\"," +
                "\"userPin\":\"huyanxia\"" +
                "}";

        String json8 = "{" +
                "\"depCity\":\"上海\"," +
                "\"queryDate\":\"2018-06-23 23:00:00\"," +
                "\"userPin\":\"huyanxia5\"" +
                "}";

        String json9 = "{" +
                "\"depCity\":\"北京\"," +
                "\"arrCity\":\"上海\"," +
                "\"queryDate\":\"2018-06-22 00:00:00\"," +
                "\"userPin\":\"huyanxia\"" +
                "}";

        String json10 = "{" +
                "\"depCity\":\"北京\"," +
                "\"arrCity\":\"三亚\"," +
                "\"queryDate\":\"2018-06-21 00:00:00\"," +
                "\"userPin\":\"huyanxia1\"" +
                "}";

        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"1",json1);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"2",json2);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"3",json3);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"4",json4);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"5",json5);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"6",json6);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"7",json7);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"8",json8);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"9",json9);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"10",json10);

    }


    /**
     * 用java字符串创建document
     */
    public static void indexWithStr(String indexName, String typeName,String docId,String json) {
        //手工构建json字符串

        //指定索引名称，type名称和documentId(documentId可选，不设置则系统自动生成)创建document
        //IndexResponse response = client.prepareIndex(indexName, typeName)
        IndexResponse response = Tool.CLIENT.prepareIndex(indexName, typeName, docId)
                .setSource(json)
                .execute()
                .actionGet();

        logger.info(response);

        //response中返回索引名称，type名称，doc的Id和版本信息
/*        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        boolean created = response.isCreated();
        System.out.println(index+","+type+","+id+","+version+","+created);*/
    }


    /**
     * 用java字符串创建document  不指定文档ID
     */
    public static void indexWithStr(String indexName, String typeName,String json) {
        //手工构建json字符串

        //指定索引名称，type名称和documentId(documentId可选，不设置则系统自动生成)创建document
        //IndexResponse response = client.prepareIndex(indexName, typeName)
        IndexResponse response = Tool.CLIENT.prepareIndex(indexName, typeName)
                .setSource(json)
                .execute()
                .actionGet();

/*        logger.info(response);

        //response中返回索引名称，type名称，doc的Id和版本信息
        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        boolean created = response.isCreated();
        System.out.println(index+","+type+","+id+","+version+","+created);*/
        logger.info(response.getId());
    }
}
