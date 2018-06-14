package com.jd.es.example;

import com.jd.es.example.common.*;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Title: Main  准备测试数据
 * Description: Main
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/15
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class Main {

    private static Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {

        try {

            //如果测试环境的索引不存在，或者文档有更改，影响运行测试例子，运行下面三个方法初始化索引，和写入供测试的文档
            //DeleteIndex.testDeleteIndex(Tool.INDEX_NAME);//删除索引
            //CreateIndex.createIndex(Tool.INDEX_NAME, Tool.TYPE_NAME);//创建索引，mapping
            IndexDoc.prepareTestDoc();//准备测试数据，向索引 agg_index 的类型 agg_type 写入8个文档

            //根据文档id批量删除文档
            //DeleteDoc.bulkDeleteDoc("string_test_index", "type1", new String[]{"AVfakzvt1qrH45UKETs0","AVfak0661qrH45UKETs1","AVfak10G1qrH45UKETs2"});

            //一个一般查询请求的示例
           //QueryBuilder qb = QueryBuilders.matchAllQuery();
            //QueryBuilder qb1 = QueryBuilders.matchQuery();

           /*QueryBuilder queryBuilder1 = QueryBuilders.matchQuery("depCity", "上海");
            QueryBuilder queryBuilder2 = QueryBuilders.matchQuery("arrCity", "北京");

            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.must(queryBuilder1);
            boolQueryBuilder.must(queryBuilder2);

            SearchRequestBuilder srb = Tool.CLIENT.prepareSearch(Tool.INDEX_NAME).setTypes(Tool.TYPE_NAME).setQuery(boolQueryBuilder).setSize(20);//setSize(3) 可以设置查询的文档数的大小，默认10
            logger.info("查询请求：\n"  + srb.toString());
            SearchResponse sResponse = srb.get();
            logger.info("查询结果：\n"  + sResponse.toString());
            logger.info("查询结果：\n"  + sResponse.getHits().getHits()[0].getSource().get("depCity"));*/
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date date = new Date();
            String endTime = simpleDateFormat.format(date);
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.must(QueryBuilders.rangeQuery("queryDate").format("yyyy-MM-dd").gt("2018-06-21").lt("2018-06-24"));

           TermsBuilder teamAgg= AggregationBuilders.terms("depCity_count ").field("depCity").subAggregation(AggregationBuilders.count("depCity_count").field("depCity")).order(Terms.Order.aggregation("depCity_count", false));
            /*AggregationBuilders.dateRange("agg")
                    .field("queryDate")
                    .addUnboundedTo("1970").addRange("1970", "2000")
                    .addRange("2000", "2010").addUnboundedFrom("2009");*/
            TermsBuilder dateAgg= AggregationBuilders.terms("queryDate_count").field("queryDate").order(Terms.Order.term(true)).subAggregation(teamAgg);
            //TermsBuilder posAgg= AggregationBuilders.dateHistogram("query_count").field("queryDate").subAggregation(AggregationBuilders.count("queryDate").field("arrCity")).order(Terms.Order.aggregation("arrCity_count", true));

            //SearchRequestBuilder srb = Tool.CLIENT.prepareSearch(Tool.INDEX_NAME).setTypes(Tool.TYPE_NAME).setQuery(boolQueryBuilder).setSize(20);//setSize(3) 可以设置查询的文档数的大小，默认10

            SearchRequestBuilder srb = Tool.CLIENT.prepareSearch(Tool.INDEX_NAME).setTypes(Tool.TYPE_NAME).setQuery(boolQueryBuilder).addAggregation(dateAgg).setSize(20);//setSize(3) 可以设置查询的文档数的大小，默认10

            logger.info("查询请求：\n"  + srb.toString());
            SearchResponse sResponse = srb.get();
            logger.info("查询结果：\n"  + sResponse.toString());
            //logger.info("查询结果：\n"  + sResponse.getHits().getHits()[0].getSource().get("depCity"));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }



    /**
     * 创建索引
     * @param indexName
     * @param documentType
     * @throws IOException
     */
    private static void createIndex(String indexName, String documentType) throws IOException {

        //先判断索引是否存在
        final IndicesExistsResponse iRes = Tool.CLIENT.admin().indices().prepareExists(indexName).execute().actionGet();
        if (iRes.isExists()) {
            Tool.CLIENT.admin().indices().prepareDelete(indexName).execute().actionGet();
        } else {
            logger.info("索引已经存在" + indexName);
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
                .field("store", true)
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()

                .startObject("dateOfBirth")
                .field("type", "date")
                .field("store", true)
                .field("index", "not_analyzed")
                //2015-08-21T08:35:13.890Z
                .field("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSSZ")
                .endObject()

                .endObject()
                .endObject()
                .endObject();

        Tool.CLIENT.admin().indices()
                .preparePutMapping(indexName)
                .setType(documentType)
                .setSource(mapping)
                .execute().actionGet();
    }

}