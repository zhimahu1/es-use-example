package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: DisMaxQuery
 * Description: DisMaxQuery 相比使用bool查询，我们可以使用dis_max查询(Disjuction Max Query)。
 * Disjuction的意思"OR"(而Conjunction的意思是"AND")，
 * 因此Disjuction Max Query的意思就是返回匹配了任何查询的文档，并且分值是产生了最佳匹配的查询所对应的分值：
 * 参考：http://blog.csdn.net/dm_vincent/article/details/41820537
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class DisMaxQuery {

    private static Logger logger = Logger.getLogger(DisMaxQuery.class);

    public static void main(String[] args) {
        testDisMaxQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "gender", "male", "country", "China");

        testDisMaxQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "age", "3", "country", "China");
    }

    /**
     * tie_breaker参数会让dis_max查询的行为更像是dis_max和bool的一种折中。它会通过下面的方式改变分值计算过程：
     取得最佳匹配查询子句的_score。
     将其它每个匹配的子句的分值乘以tie_breaker。
     将以上得到的分值进行累加并规范化。
     通过tie_breaker参数，所有匹配的子句都会起作用，只不过最佳匹配子句的作用更大。
     tie_breaker的取值范围是0到1之间的浮点数，取0时即为仅使用最佳匹配子句(译注：和不使用tie_breaker参数的dis_max查询效果相同)，取1则会将所有匹配的子句一视同仁。它的确切值需要根据你的数据和查询进行调整，但是一个合理的值会靠近0，(比如，0.1 -0.4)，来确保不会压倒dis_max查询具有的最佳匹配性质。
     */

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "dis_max": {
                  "tie_breaker": 0.7,
                  "boost": 1.2,
                  "queries": [
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
    private static void testDisMaxQuery(String indexName, String typeName, String fieldName, String fieldValue, String fieldName2, String fieldValue2) {

        QueryBuilder qb = QueryBuilders.disMaxQuery()
                .add(QueryBuilders.termQuery(fieldName, fieldValue))
                .add(QueryBuilders.termQuery(fieldName2, fieldValue2))
                .boost(1.2f)
                .tieBreaker(0.7f);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
