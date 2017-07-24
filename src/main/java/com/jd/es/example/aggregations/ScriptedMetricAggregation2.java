package com.jd.es.example.aggregations;

import com.jd.es.example.common.DeleteDoc;
import com.jd.es.example.common.IndexDoc;
import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;

/**
 * Title: ScriptedMetricAggregation
 * Description: ScriptedMetricAggregation
 * Reference：https://www.elastic.co/guide/en/elasticsearch/reference/2.1/search-aggregations-metrics-scripted-metric-aggregation.html
 *            https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.1/_metrics_aggregations.html
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class ScriptedMetricAggregation2 {
    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.ScriptedMetricAggregation2.class);

    public static void main(String[] args) {

        try {
            //如下先创建测试数据
            //CreateIndex.createIndexNoMapping("transactions", "stock",2,0);//创建一个具有2分片的索引
            //indexDoc();//插入四个文档

            /*四步走：
                1.init_script
                    Executed prior to any collection of documents.
                2.map_script
                    Executed once per document collected.
                3.combine_script
                    Executed once on each shard after document collection is complete.Allows the aggregation to consolidate the state returned from each shard.
                4.reduce_script
                    Executed once on the coordinating node after all shards have returned their results.
            */
            scriptedMetricAggregation("transactions", "stock");
            scriptedMetricAggregationWithCombineScript("transactions", "stock");
            scriptedMetricAggregationWithReduceScript("transactions", "stock");
            /* 输出：
                    scriptedResult [{transactions=[-30]}, {transactions=[80, -10, 130]}]
                    scriptedResult [-30, 200]
                    scriptedResult 170
            */

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void scriptedMetricAggregation(String indexName, String typeName) {

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .scriptedMetric("agg")
                        .initScript(new Script("_agg['transactions'] = []"))
                        .mapScript(new Script("if (doc['type'].value == \"sale\") { _agg.transactions.add(doc['amount'].value) } else { _agg.transactions.add(-1 * doc['amount'].value) }"));

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        ScriptedMetric agg = sResponse.getAggregations().get("agg");
        Object scriptedResult = agg.aggregation();
        logger.info("scriptedResult " + scriptedResult);
    }

    private static void scriptedMetricAggregationWithCombineScript(String indexName, String typeName) {

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .scriptedMetric("agg")
                        .initScript(new Script("_agg['transactions'] = []"))
                        .mapScript(new Script("if (doc['type'].value == \"sale\") { _agg.transactions.add(doc['amount'].value) } else { _agg.transactions.add(-1 * doc['amount'].value) }"))
                        .combineScript(new Script("profit = 0; for (t in _agg.transactions) { profit += t }; return profit"));


        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        ScriptedMetric agg = sResponse.getAggregations().get("agg");
        Object scriptedResult = agg.aggregation();
        logger.info("scriptedResult " + scriptedResult);
    }

    private static void scriptedMetricAggregationWithReduceScript(String indexName, String typeName) {

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .scriptedMetric("agg")
                        .initScript(new Script("_agg['transactions'] = []"))
                        .mapScript(new Script("if (doc['type'].value == \"sale\") { _agg.transactions.add(doc['amount'].value) } else { _agg.transactions.add(-1 * doc['amount'].value) }"))
                        .combineScript(new Script("profit = 0; for (t in _agg.transactions) { profit += t }; return profit"))
                        .reduceScript(new Script("profit = 0; for (a in _aggs) { profit += a }; return profit"));

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        ScriptedMetric agg = sResponse.getAggregations().get("agg");
        Object scriptedResult = agg.aggregation();
        logger.info("scriptedResult " + scriptedResult);
    }


    /*
        $ curl -XPUT 'http://localhost:9200/transactions/stock/1' -d '
        {
            "type": "sale",
            "amount": 80
        }
        '
        $ curl -XPUT 'http://localhost:9200/transactions/stock/2' -d '
        {
            "type": "cost",
            "amount": 10
        }
        '
        $ curl -XPUT 'http://localhost:9200/transactions/stock/3' -d '
        {
            "type": "cost",
            "amount": 30
        }
        '
        $ curl -XPUT 'http://localhost:9200/transactions/stock/4' -d '
        {
            "type": "sale",
            "amount": 130
        }
     */
    private static void indexDoc() {

        String json1 = "{\n" +
                "    \"type\": \"sale\",\n" +
                "    \"amount\": 80\n" +
                "}";
        String json2 = "{\n" +
                "    \"type\": \"cost\",\n" +
                "    \"amount\": 10\n" +
                "}";
        String json3 = "{\n" +
                "    \"type\": \"cost\",\n" +
                "    \"amount\": 30\n" +
                "}";
        String json4 = "{\n" +
                "    \"type\": \"sale\",\n" +
                "    \"amount\": 130\n" +
                "}";

        IndexDoc.indexWithStr("transactions", "stock", "1", json1);
        IndexDoc.indexWithStr("transactions", "stock", "2", json2);
        IndexDoc.indexWithStr("transactions", "stock", "3", json3);
        IndexDoc.indexWithStr("transactions", "stock", "4", json4);
    }


    private static void deleteDoc() {
        DeleteDoc.deleteDocWithId("transactions", "stock", "1");
        DeleteDoc.deleteDocWithId("transactions", "stock", "2");
        DeleteDoc.deleteDocWithId("transactions", "stock", "3");
        DeleteDoc.deleteDocWithId("transactions", "stock", "4");
    }
}
