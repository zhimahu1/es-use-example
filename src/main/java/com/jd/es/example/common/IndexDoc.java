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
                "\"name\":\"c1\"," +
                "\"gender\":\"male\"," +
                "\"country\":\"China\"," +
                "\"age\":\"1\"," +
                "\"height\":\"10.0\"," +
                "\"dateOfBirth\":\"2010-08-19 09:00:00\"" +
                "}";

        String json2 = "{" +
                "\"name\":\"c2\"," +
                "\"gender\":\"male\"," +
                "\"country\":\"China\"," +
                "\"age\":\"2\"," +
                "\"height\":\"20.0\"," +
                "\"dateOfBirth\":\"2011-08-19 09:00:00\"" +
                "}";

        String json3 = "{" +
                "\"name\":\"c3\"," +
                "\"gender\":\"female\"," +
                "\"country\":\"China\"," +
                "\"age\":\"3\"," +
                "\"height\":\"30.0\"," +
                "\"dateOfBirth\":\"2012-08-19 09:00:00\"" +
                "}";

        String json4 = "{" +
                "\"name\":\"c4\"," +
                "\"gender\":\"female\"," +
                "\"country\":\"China\"," +
                "\"age\":\"4\"," +
                "\"height\":\"40.0\"," +
                "\"dateOfBirth\":\"2012-08-19 09:00:00\"" +
                "}";

        String json5 = "{" +
                "\"name\":\"a5\"," +
                "\"gender\":\"male\"," +
                "\"country\":\"American\"," +
                "\"age\":\"5\"," +
                "\"height\":\"50.0\"," +
                "\"dateOfBirth\":\"2013-08-19 09:00:00\"" +
                "}";

        String json6 = "{" +
                "\"name\":\"a6\"," +
                "\"gender\":\"male\"," +
                "\"country\":\"American\"," +
                "\"age\":\"6\"," +
                "\"height\":\"60.0\"," +
                "\"dateOfBirth\":\"2014-08-19 09:00:00\"" +
                "}";

        String json7 = "{" +
                "\"name\":\"a7\"," +
                "\"gender\":\"female\"," +
                "\"country\":\"American\"," +
                "\"age\":\"7\"," +
                "\"height\":\"70.0\"," +
                "\"dateOfBirth\":\"2015-08-19 09:00:00\"" +
                "}";

        String json8 = "{" +
                "\"name\":\"a8\"," +
                "\"gender\":\"female\"," +
                "\"country\":\"American\"," +
                "\"age\":\"8\"," +
                "\"height\":\"80.0\"," +
                "\"dateOfBirth\":\"2015-08-19 09:00:00\"" +
                "}";


        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"1",json1);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"2",json2);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"3",json3);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"4",json4);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"5",json5);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"6",json6);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"7",json7);
        indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME,"8",json8);
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
