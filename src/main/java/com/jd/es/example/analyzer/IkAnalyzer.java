package com.jd.es.example.analyzer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jd.es.example.common.CreateIndex;
import com.jd.es.example.common.IndexDoc;
import com.jd.es.example.common.SimpleHttpClient;
import com.jd.es.example.common.Tool;
import com.jd.es.example.query.MatchQuery;
import com.jd.es.example.query.TermQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Title: IkAnalyzer ik中文分词器的使用，测试
 * Description: ikAnalyzer
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2017/2/24
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class IkAnalyzer {

    private static Logger logger = LoggerFactory.getLogger(IkAnalyzer.class);

    private static String IndexName = "air_flight_ikindex";
    private static String TypeName = "air_flight_iktype";

    /* ik可以对中文，也可以对英文分词
    ik_test/_analyze?analyzer=ik
    {
        "text": "Good Morning!I am LiLei,小明喜欢小红花。"
    }
    */

    public static void main(String[] args) {

        //删除索引
        //DeleteIndex.testDeleteIndex(IndexName);

        //创建索引和mapping
        //createIndexAndMapping();

        //写入文档
        indexDoc();

        //比较分词结果
        String s1 = "小明是中华人民共和国的公民";
        String s2 = "如果明天天气好的话，小明和小红一起去人民公园赏樱花。";
        String s3 = "小明喜欢小红，但是小红喜欢小华。";
        System.out.println(Arrays.toString(showIkAnalyzerResult(s1)));//ik分词器
        System.out.println(Arrays.toString(showEnglishAnalyzerResult(s1)));//英文分词会将中文分成一个一个的字
        System.out.println(Arrays.toString(showIkAnalyzerResult(s2)));
        System.out.println(Arrays.toString(showEnglishAnalyzerResult(s2)));
        System.out.println(Arrays.toString(showIkAnalyzerResult(s3)));
        System.out.println(Arrays.toString(showEnglishAnalyzerResult(s3)));

        //MatchQuery.testMatchQuery(IndexName,TypeName,"ik_field","小");//匹配不到任何文档
        //MatchQuery.testMatchQuery(IndexName,TypeName,"english_field","小");//可以匹配到所有含有"小"字的文档。因为英文分词按字切分。

        MatchQuery.testMatchQuery(IndexName,TypeName,"ik_field","小华");
        TermQuery.testTermQuery(IndexName,TypeName,"ik_field","小华");

        //MatchQuery.testMatchQuery(IndexName,TypeName,"ik_field","小红喜欢小华");//查找含有"小红","喜欢","小华"这三个term的文档，匹配越多，打分越高
        //TermQuery.testTermQuery(IndexName,TypeName,"ik_field","小红喜欢小华");//匹配不到任何文档，因为倒排索引中没有"小红喜欢小华"这个term

    }


    //创建索引和mapping
    private static void createIndexAndMapping() {

        //1.创建索引 mapping，指定分词器"analyzer": "ik"
        /**
        {
            "mappings": {
            "type_name": {
                "properties": {
                    "english_field": {
                        "type": "string",
                                "analyzer": "english"
                    },
                    "ik_field": {
                        "type": "string",
                                "analyzer": "ik"
                    }
                }
            }
        }
        }
         */
        try {
            CreateIndex.createIndexWithMapping(IndexName, "{\"mappings\":{\"type_name\":{\"properties\":{\"english_field\":{\"type\":\"string\",\"analyzer\":\"english\"},\"ik_field\":{\"type\":\"string\",\"analyzer\":\"ik\"}}}}}");
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }


    //插入文档
    private static void indexDoc() {
        IndexDoc.indexWithStr(IndexName, TypeName, "1", "{\"name\":\"小明是中华人民共和国的公民\",\"myname\":\"小明是中华人民共和国的公民\"}");
        IndexDoc.indexWithStr(IndexName, TypeName, "2", "{\"name\":\"如果明天天气好的话，小明和小红一起去人民公园赏樱花。\",\"myname\":\"如果明天天气好的话，小明和小红一起去人民公园赏樱花。\"}");
        IndexDoc.indexWithStr(IndexName, TypeName, "3", "{\"name\":\"小明喜欢小红，但是小红喜欢小华。\",\"myname\":\"小明喜欢小红，但是小红喜欢小华。\"}");
    }


    //查看ik分词后的结果
    private static String[] showIkAnalyzerResult(String strToAnalyze) {
        return showAnalyzerResult(IndexName,"ik", strToAnalyze);
    }

    //查看english分词后的结果
    private static String[] showEnglishAnalyzerResult(String strToAnalyze) {
        return showAnalyzerResult(IndexName,"english", strToAnalyze);
    }

    /**
    ik_test/_analyze?analyzer=ik
    {
        "text":"小明喜欢小红"
    }
     */
    public static String[] showAnalyzerResult(String indexName,String analyzer, String strToAnalyze) {
        try {
            String queryBody = "{\"text\":\"" + strToAnalyze + "\"}";
            String url = Tool.URL + "/" + indexName + "/_analyze?analyzer=" + analyzer;
            String res = SimpleHttpClient.post(url, queryBody);
            if (res != null && !res.isEmpty()) {
                JSONObject jsonObject = JSON.parseObject(res);
                JSONArray tokens = jsonObject.getJSONArray("tokens");
                String[] tokensArr = new String[tokens.size()];
                for (int i = 0; i < tokens.size(); i++) {
                    tokensArr[i] = tokens.getJSONObject(i).getString("token");
                }
                return tokensArr;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
