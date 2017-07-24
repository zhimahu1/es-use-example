package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: BoolQuery
 * Description: BoolQuery   https://www.elastic.co/guide/en/elasticsearch/reference/2.1/query-dsl-bool-query.html
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class TestQuery {

    private static Logger logger = Logger.getLogger(TestQuery.class);

    public static void main(String[] args) {
        testBoolQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "name", "c1", "gender", "male","age","1", "country", "China");

    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "bool": {
                  "must": {
                    "term": {
                      "gender": "male"
                    }
                  },
                  "filter": {
                    "term": {
                      "gender": "male"
                    }
                  },
                  "must_not": {
                    "range": {
                      "age": {
                        "from": 10,
                        "to": 20
                      }
                    }
                  },
                  "should": [
                    {
                      "term": {
                        "height": "50"
                      }
                    },
                    {
                      "term": {
                        "country": "China"
                      }
                    }
                  ],
                  "minimum_should_match": 1,
                  "boost": 1
                }
              }
            }
     */
    private static void testBoolQuery(String indexName, String typeName, String fieldName1, String fieldValue1, String fieldName2, String fieldValue2, String fieldName3, String fieldValue3, String fieldName4, String fieldValue4) {

        QueryBuilder q1 = QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery(fieldName2, fieldValue2))
                .should(QueryBuilders.termQuery(fieldName2, fieldValue2))
                .minimumNumberShouldMatch(1);

        QueryBuilder q2 = QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery(fieldName2, fieldValue2))
                .should(QueryBuilders.termQuery(fieldName2, fieldValue2))
                .minimumNumberShouldMatch(1);

        QueryBuilder qb = QueryBuilders.boolQuery()
                //.filter(QueryBuilders.termQuery("gender", "male"))
                .must(q1)
                .must(q2);

        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb);
        logger.info(searchRequestBuilder);
        SearchResponse sResponse = searchRequestBuilder.get();
        logger.info(sResponse);
    }


    private static void testBoolFilterQuery(String indexName, String typeName, String fieldName1, String fieldValue1, String fieldName2, String fieldValue2) {

        QueryBuilder qb = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(fieldName1, fieldValue1))
                .filter(QueryBuilders.termQuery(fieldName2, fieldValue2));
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }

}
