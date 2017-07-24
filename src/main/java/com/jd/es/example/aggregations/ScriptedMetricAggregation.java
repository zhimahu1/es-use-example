package com.jd.es.example.aggregations;


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
 * Reference:https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.1/_metrics_aggregations.html
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class ScriptedMetricAggregation {
    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.ScriptedMetricAggregation.class);

    public static void main(String[] args) {

        try {
            scriptedMetricAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME);
            scriptedMetricAggregationWithCombineScript(Tool.INDEX_NAME, Tool.TYPE_NAME);
            scriptedMetricAggregationWithReduceScript(Tool.INDEX_NAME, Tool.TYPE_NAME);
            /* 输出：
                    scriptedResult[{heights=[10.0, -30.0, -40.0, 50.0, 60.0, -70.0, -80.0, 20.0]}]
                    scriptedResult[-80.0]
                    scriptedResult-80.0
            */
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void scriptedMetricAggregation(String indexName, String typeName) {

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .scriptedMetric("agg")
                        .initScript(new Script("_agg['heights'] = []"))
                        .mapScript(new Script("if (doc['gender'].value == \"male\") " +
                                "{ _agg.heights.add(doc['height'].value) } " +
                                "else " +
                                "{ _agg.heights.add(-1 * doc['height'].value) }"));

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        ScriptedMetric agg = sResponse.getAggregations().get("agg");
        Object scriptedResult = agg.aggregation();
        logger.info("scriptedResult" + scriptedResult);
    }

    private static void scriptedMetricAggregationWithCombineScript(String indexName, String typeName) {

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .scriptedMetric("agg")
                        .initScript(new Script("_agg['heights'] = []"))
                        .mapScript(new Script("if (doc['gender'].value == \"male\") " +
                                "{ _agg.heights.add(doc['height'].value) } " +
                                "else " +
                                "{ _agg.heights.add(-1 * doc['height'].value) }"))
                        .combineScript(new Script("heights_sum = 0; for (t in _agg.heights) { heights_sum += t }; return heights_sum"));


        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        ScriptedMetric agg = sResponse.getAggregations().get("agg");
        Object scriptedResult = agg.aggregation();
        logger.info("scriptedResult" + scriptedResult);
    }

    private static void scriptedMetricAggregationWithReduceScript(String indexName, String typeName) {

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .scriptedMetric("agg")
                        .initScript(new Script("_agg['heights'] = []"))
                        .mapScript(new Script("if (doc['gender'].value == \"male\") " +
                                "{ _agg.heights.add(doc['height'].value) } " +
                                "else " +
                                "{ _agg.heights.add(-1 * doc['height'].value) }"))
                        .combineScript(new Script("heights_sum = 0; for (t in _agg.heights) { heights_sum += t }; return heights_sum"))
                        .reduceScript(new Script("heights_sum = 0; for (a in _aggs) { heights_sum += a }; return heights_sum"));

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        ScriptedMetric agg = sResponse.getAggregations().get("agg");
        Object scriptedResult = agg.aggregation();
        logger.info("scriptedResult" + scriptedResult);
    }
}
