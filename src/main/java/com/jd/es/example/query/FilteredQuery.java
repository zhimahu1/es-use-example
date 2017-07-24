package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: FilteredQuery 不推荐使用，可以用bool query代替。FilteredQuery查询起来很慢很慢，而且，2.1已经废弃。
 * Description: FilteredQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
@Deprecated
public class FilteredQuery {

    private static Logger logger = Logger.getLogger(FilteredQuery.class);

    public static void main(String[] args) {
        testFilteredQuery(Tool.INDEX_NAME, Tool.TYPE_NAME);
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "filtered": {
                  "query": {
                    "match": {
                      "country": {
                        "query": "China",
                        "type": "boolean"
                      }
                    }
                  },
                  "filter": {
                    "range": {
                      "age": {
                        "from": "2",
                        "to": "6",
                        "include_lower": true,
                        "include_upper": true
                      }
                    }
                  }
                }
              }
            }
     */
    private static void testFilteredQuery(String indexName, String typeName) {

    /**
     * Use the bool query instead with a must clause for the query and a filter clause for the filter.
     */
        QueryBuilder qb = QueryBuilders.filteredQuery(
                QueryBuilders.matchQuery("country", "China"),//be used for scoring
                QueryBuilders.rangeQuery("age").from("2").to("6")//only be used for filtering the result set
        );
        logger.info(qb);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }




/** 这种写法有问题，filter中写多个条件的话，后面会覆盖前面的，只有后面的会生效。不要这么用！
    {
        "query": {
        "filtered": {
            "query": {
                "match_all": {}
            },
            "filter": [
            {
                "range": {
                "age": {
                    "from": 1,
                            "to": 3,
                            "include_lower": true,
                            "include_upper": true
                }
            }
            },
            {
                "term": {
                "name": "a"
            }
            }
            ]
        }
    }
    }
*/
}