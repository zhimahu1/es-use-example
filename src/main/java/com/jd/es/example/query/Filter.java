package com.jd.es.example.query;

/**文档地址：  https://www.elastic.co/guide/en/elasticsearch/reference/2.1/query-filter-context.html

query content 和 filter content
        查询子句的表现取决于它是在query content里还是在filter content里。

1.query content
        query content中的查询子句回答问题"文档匹配的程度如何"。计算_score打分表示匹配程度。
        所谓查询上下文，是指包含一个或多个查询条件的逻辑组合，QueryContext里包含的的单个查询表示的含义是“文档和该查询条件的匹配度有多大”，针对QueryContext里的每一个查询都会计算出一个_score来表示匹配度，整个QueryContext的匹配度评分等于其包含所有单个查询评分的总和，所以针对一个Query我们更多关注的是匹配度。

2.filter content
        filter content中的查询子句回答问题"文档是否匹配查询子句"，回答是简单的"YES"或者"NO"，不打分。
        经常使用的filters将会自动被ES缓存来提高性能。
        Filter context is in effect whenever a query clause is passed to a filter parameter,such as the filter or must_not parameters in the bool query,
            the filter parameter in the constant_score query, or the filter aggregation.
        过滤器上下文和查询上下文类似，都是由1个或多个子查询(Query clause) 组成的。他们的区别在于Filter不会计算文档得分，它代表的含义是“文档和该查询条件是否匹配”，所以针对一个Filter我们更多关注的是能否可以过滤掉文档，返回的结果只能是可以或者不可以（true or false）。

 Query context
    A query used in query context will calculate relevance scores and will not be cacheable. Query context is used whenever filter context does not apply.

 Filter context
    A query used in filter context will not calculate relevance scores, and will be cacheable. Filter context is introduced by:
        the constant_score query
        the must_not and (newly added) filter parameter in the bool query
        the filter and filters parameters in the function_score query
        any API called filter, such as the post_filter search parameter, or in aggregations or index aliases

 TODO
 https://www.elastic.co/guide/en/elasticsearch/reference/2.1/breaking_20_query_dsl_changes.html#_queries_and_filters_merged

*/

import com.jd.es.example.aggregations.FilterAggregation;
import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

/**
 * Title: Filter
 * Description: Filter
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/10/14
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class Filter {

    private static Logger logger = Logger.getLogger(Filter.class);

    public static void main(String[] args) {
        testFilterInBoolQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "gender", "male", "country", "China");

        testFilterInAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME);

        testFilterInConstantScoreQuery();
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "bool": {
                  "filter": [
                    {
                      "term": {
                        "gender": "male"
                      }
                    },
                    {
                      "term": {
                        "country": "China"
                      }
                    }
                  ]
                }
              }
            }
     */
    private static void testFilterInBoolQuery(String indexName, String typeName, String fieldName1, String fieldValue1, String fieldName2, String fieldValue2) {

        QueryBuilder qb = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(fieldName1, fieldValue1))
                .filter(QueryBuilders.termQuery(fieldName2, fieldValue2));
        logger.info(qb);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }


    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size":0,
              "aggregations": {
                "agg_my": {
                  "filter": {
                    "term": {
                      "gender": "female"
                    }
                  },
                  "aggregations": {
                    "avg_age": {
                      "avg": {
                        "field": "age"
                      }
                    }
                  }
                }
              }
            }
     */
    private static void testFilterInAggregation(String indexName, String typeName) {

        AbstractAggregationBuilder aggregation =
                AggregationBuilders
                        .filter("agg_my")
                        .filter(QueryBuilders.termQuery("gender", "female"))
                        .subAggregation(AggregationBuilders.avg("avg_age").field("age"));

        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).setSize(0);
        logger.info(searchRequestBuilder);//打印请求
        SearchResponse sResponse = searchRequestBuilder.get();
        logger.info(sResponse);//打印结果
    }



    public static void testFilterInConstantScoreQuery(){
        ConstantScoreQuery.testConstantScoreQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "name", "c1");
    }

}
