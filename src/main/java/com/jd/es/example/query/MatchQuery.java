package com.jd.es.example.query;

import com.jd.es.example.common.CreateIndex;
import com.jd.es.example.common.DeleteIndex;
import com.jd.es.example.common.IndexDoc;
import com.jd.es.example.common.Tool;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Title: MatchQuery
 * Description: MatchQuery
 *      https://www.elastic.co/guide/en/elasticsearch/reference/2.1/query-dsl-match-query.html
 *      http://106.186.120.253/preview/match-query.html
 *      https://www.elastic.co/guide/en/elasticsearch/reference/2.1/query-dsl-match-query.html
 *
 * 像 match 或 query_string 这样的查询是高层查询，它们了解字段映射的信息：
 *      1.如果查询 日期（date） 或 整数（integer） 字段，它们会将查询字符串分别作为日期或整数对待。
 *      2.如果查询一个（ not_analyzed ）未分析的精确值字符串字段， 它们会将整个查询字符串作为单个词项对待。
 *      3.但如果要查询一个（ analyzed ）已分析的全文字段， 它们会先将查询字符串传递到一个合适的分析器，然后生成一个供查询的词项列表。
 *  一旦组成了词项列表，这个查询会对每个词项逐一执行底层的查询，再将结果合并，然后为每个文档生成一个最终的相关度评分。
 *
 *  match查询其实底层是多个term查询，最后将term的结果合并
 *
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/11/23
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class MatchQuery {

    private static Logger logger = LoggerFactory.getLogger(MatchQuery.class);

    private static String IndexName = "match_test";
    private static String TypeName = "type_name";

    public static void main(String[] args){

        //删除索引
        //DeleteIndex.testDeleteIndex(IndexName);

        //创建索引和mapping
        //createIndexAndMapping();

        //写入文档
        //indexDoc();

        testMatchQuery(IndexName,TypeName,"name","abc");//1个文档
        testMatchQuery(IndexName,TypeName,"name","ABC");//1个文档
        testMatchQuery(IndexName,TypeName,"name","abc ABC");//name不分词,所以查不出来
        testMatchQuery(IndexName,TypeName,"description","football");//description分词，所以大写，小写都能查出来

        testMatchQuery(IndexName,TypeName,"description","Table Tennis");//description分词，所以大写，小写都能查出来,只包含table的也能查出来
        testMatchQueryWithOperate(IndexName,TypeName,"description","Table Tennis",Operator.AND);//只包含table的查不出来

        testMatchQueryWithMinimumShouldMatch(IndexName,TypeName,"description","Table Tennis","1");//匹配3个文档，只包含table的也匹配
        testMatchQueryWithMinimumShouldMatch(IndexName,TypeName,"description","Table Tennis","2");//匹配2个文档

        testMatchQueryWithMinimumShouldMatch(IndexName,TypeName,"description","football,playing,music","75%");//匹配3个文档
    }


    //创建索引和mapping
    private static void createIndexAndMapping() {

        //1.创建索引 mapping
        /**
        {
            "mappings": {
            "type_name": {
                "properties": {
                    "name": {
                        "type": "string",
                                "index": "not_analyzed"
                    },
                    "description": {
                        "type": "string"
                    }
                }
            }
        }
        }
         */
        try {
            CreateIndex.createIndexWithMapping(IndexName, "{\"mappings\":{\"type_name\":{\"properties\":{\"name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"description\":{\"type\":\"string\"}}}}}");
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    //插入文档
    private static void indexDoc() {
        IndexDoc.indexWithStr(IndexName, TypeName, "1", "{\"name\":\"abc\",\"description\":\"football,basketball,music,skating,table tennis\"}");
        IndexDoc.indexWithStr(IndexName, TypeName, "2", "{\"name\":\"ABC\",\"description\":\"Football,basketball\"}");
        IndexDoc.indexWithStr(IndexName, TypeName, "3", "{\"name\":\"abcdef\",\"description\":\"football,music,skating,table tennis\"}");
        IndexDoc.indexWithStr(IndexName, TypeName, "4", "{\"name\":\"bc\",\"description\":\"Football,badminton,table playing\"}");
    }

    //将gender设置成analyzed
    //  agg_index/_search
    /**
    {
        "query": {
        "match": {
            "gender": "Male"
        }
    }
    }
    */
    public static void testMatchQuery(String indexName, String typeName, String fieldName, String fieldValue){
        try{
            QueryBuilder qb = QueryBuilders.matchQuery(fieldName, fieldValue);
            SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
            logger.info(sResponse.toString());
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    /**
     * @param indexName
     * @param typeName
     * @param fieldName
     * @param fieldValue
     * @param op Operator枚举量有（ OR, AND）
     // match_test/type_name/_search
    {
        "query": {
        "match": {
            "description": {
                "query": "Table Tennis",
                        "operator": "AND"
            }
        }
    }
    }
     */
    public static void testMatchQueryWithOperate(String indexName, String typeName, String fieldName, String fieldValue,Operator op){
        try{
            QueryBuilder qb = QueryBuilders.matchQuery(fieldName, fieldValue).operator(op);
            SearchRequestBuilder srb = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb);
            //logger.info(srb.toString());
            SearchResponse sResponse = srb.get();
            logger.info(sResponse.toString());
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     *
     * @param indexName
     * @param typeName
     * @param fieldName
     * @param fieldValue
     * @param minimumShouldMatch 可以是 75% 或者是 2
     */
    public static void testMatchQueryWithMinimumShouldMatch(String indexName, String typeName, String fieldName, String fieldValue,String minimumShouldMatch){
        try{
            QueryBuilder qb = QueryBuilders.matchQuery(fieldName, fieldValue).minimumShouldMatch(minimumShouldMatch);
            SearchRequestBuilder srb = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb);
            //logger.info(srb.toString());
            SearchResponse sResponse = srb.get();
            logger.info(sResponse.toString());
        }catch(Exception e){
            e.printStackTrace();
        }
    }



}
