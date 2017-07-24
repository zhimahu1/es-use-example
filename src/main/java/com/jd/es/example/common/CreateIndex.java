package com.jd.es.example.common;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Title: CreateIndex TODO 自己测试还可以，线上不允许自己随便创建索引，后果自负！要创建索引请在管理端esm.jd.com提交创建索引申请。
 * Description: CreateIndex
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
@Deprecated
public class CreateIndex {

    private static Logger logger = Logger.getLogger(CreateIndex.class);

    public static void main(String[] args){
        try {
            createIndex(Tool.INDEX_NAME, Tool.TYPE_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建索引  TODO 自己测试还可以，线上不要自己随便创建索引，后果自负！要创建索引请在管理端esm.jd.com提交创建索引申请。
     * @param indexName
     * @param documentType
     * @throws IOException
     */
    @Deprecated
    public static void createIndex(String indexName, String documentType) throws IOException {

        //先判断索引是否存在
        final IndicesExistsResponse iRes = Tool.CLIENT.admin().indices().prepareExists(indexName).execute().actionGet();
        if (iRes.isExists()) {
            //DeleteIndex.testDeleteIndex(indexName);
            logger.info("索引已经存在" + indexName + ",不创建");
            return;
        }

        Tool.CLIENT.admin().indices().prepareCreate(indexName).setSettings(Settings.settingsBuilder().put("number_of_shards", 1).put("number_of_replicas", "0")).execute().actionGet();
        XContentBuilder mapping = jsonBuilder()
                .startObject()
                .startObject(documentType)
//                     .startObject("_routing").field("path","tid").field("required", "true").endObject()
                .startObject("_source").field("enabled", "true").endObject()
                .startObject("_all").field("enabled", "false").endObject()
                .startObject("properties")

                .startObject("name")
                //.field("store", true)
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("gender")
                //.field("store", true)
                .field("type", "string")
                //.field("index", "not_analyzed")
                .endObject()

                .startObject("country")
                //.field("store", true)
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("age")
                //.field("store", true)
                .field("type", "integer")
                .endObject()

                .startObject("height")
                //.field("store", true)
                .field("type", "float")
                .endObject()

                .startObject("dateOfBirth")
                .field("type", "date")
                //.field("store", true)
                .field("index", "not_analyzed")
                        //2015-08-21T08:35:13.890Z
                .field("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSSZ")
                .endObject()

/*                .startObject("message")
                .field("store", true)
                .field("type", "string")
                .field("index", "analyzed")
                .field("analyzer", "standard")
                .endObject()*/

                .endObject()
                .endObject()
                .endObject();


        Tool.CLIENT.admin().indices()
                .preparePutMapping(indexName)
                .setType(documentType)
                .setSource(mapping)
                .execute().actionGet();
    }


    /**
     *   TODO 自己测试还可以，线上不要自己随便创建索引，后果自负！要创建索引请在管理端esm.jd.com提交创建索引申请。
     * @param indexName
     * @param shards
     * @param replicas
     * @throws IOException
     */
    @Deprecated
    public static void createIndexNoMapping(String indexName,int shards,int replicas) throws IOException {
        final IndicesExistsResponse iRes = Tool.CLIENT.admin().indices().prepareExists(indexName).execute().actionGet();
        if (iRes.isExists()) {
            Tool.CLIENT.admin().indices().prepareDelete(indexName).execute().actionGet();
        }
        Tool.CLIENT.admin().indices()
                .prepareCreate(indexName)
                .setSettings(Settings.settingsBuilder().put("number_of_shards", shards).put("number_of_replicas", replicas))
                .execute()
                .actionGet();

/*        Tool.CLIENT.admin().indices()
                .preparePutMapping(indexName)
                .setType(documentType)
                .execute().actionGet();*/
    }


    /**
     * 创建索引  TODO 自己测试还可以，线上不要自己随便创建索引，后果自负！要创建索引请在管理端esm.jd.com提交创建索引申请。
     * @param indexName
     * @param mapping 该索引的mapping
     * @throws IOException
     */
    @Deprecated
    public static void createIndexWithMapping(String indexName,String mapping) throws IOException {

        try{
            //先判断索引是否存在
            IndicesExistsResponse iRes = Tool.CLIENT.admin().indices().prepareExists(indexName).execute().actionGet();
            if (iRes.isExists()) {
                logger.info("索引已经存在" + indexName + ",不创建");
                return;
            }

            Tool.CLIENT.admin().indices().prepareCreate(indexName).setSource(mapping).get();

        }catch(Exception e){
            logger.error(e.getMessage(),e);
        }
    }

}
