package com.jd.es.example.analyzer;

import com.jd.es.example.common.*;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Title: WhitespaceTokenizer
 * Description: WhitespaceTokenizer 空格分词器
 *
 *  除了Elasticsearch 内置的分析器，你可以通过在配置文件中组合字符过滤器，分词器和标记过滤器，来满足特定的需求。
 *  一个分析器(analyzer)由三部分组成
 *  1.字符过滤器(character filter)   让字符串在被分词前变得更加“整洁”,例如去除HTML 标签，诸如 <p> 或 <div>。
 *  2.分词器(tokenizer)     分词器将字符串分割成单独的词（terms）或标记（tokens）standard 分析器使用 standard 分词器将字符串分割成单独的字词，删除大部分标点符号 keyword 分词器输出和它接收到的相同的字符串，不做任何分词处理。[whitespace 分词器]只通过空格来分割文本。[pattern 分词器]可以通过正则表达式来分割文本。
 *  3.标记过滤(token filters)     标记过滤器可能修改，添加或删除标记。如 lowercase 和 stop 标记过滤器，。stemmer 标记过滤器将单词转化为他们的根形态（root form）。ascii_folding 标记过滤器会删除变音符号，比如从 très 转为 tres。 ngram 和 edge_ngram 可以让标记更适合特殊匹配情况或自动完成。
 *
 *  参考：https://es.xiaoleilu.com/070_Index_Mgmt/20_Custom_Analyzers.html
 *
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2017/5/16
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class WhitespaceTokenizer {

    private static Logger logger = LoggerFactory.getLogger(WhitespaceTokenizer.class);

    private static String IndexName = "whitespace_tokenizer_test";
    private static String TypeName = "type_name";

/*
    GET
    whitespace_tokenizer_test/_analyze?tokenizer=whitespace
    OR
    whitespace_tokenizer_test/_analyze?analyzer=my_whitespace_tokenizer_analyzer
    {
        "text": "小明 喜欢 小 红花,小红 不喜欢 小红花"
    }*/

    public static void main(String[] args) {

        //删除索引
        //DeleteIndex.testDeleteIndex(IndexName);

        //创建索引和mapping
        createIndexAndMapping();

        //比较分词结果
        String s1 = "小明 喜欢 小 红花,小红 不喜欢 小红花。";
        System.out.println(Arrays.toString(IkAnalyzer.showAnalyzerResult(IndexName,"my_whitespace_tokenizer_analyzer", s1)));//自定义的my_whitespace_tokenizer_analyzer分词器
        System.out.println(Arrays.toString(IkAnalyzer.showAnalyzerResult(IndexName,"english", s1)));//english分词器
        System.out.println(Arrays.toString(IkAnalyzer.showAnalyzerResult(IndexName,"standard", s1)));//standard分词器
        System.out.println(Arrays.toString(IkAnalyzer.showAnalyzerResult(IndexName,"ik", s1)));//ik分词器

        //java 调用分词验证API
        try {
            AnalyzeRequest analyzeRequest = new AnalyzeRequest().index(IndexName).analyzer("ik").text(s1);
            AnalyzeResponse analyzeResponse = Tool.CLIENT.admin().indices().analyze(analyzeRequest).get();
            analyzeResponse.getTokens().forEach(analyzeToken -> {System.out.println(analyzeToken.getTerm());});;
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
        }

    }


    //创建索引和mapping
    private static void createIndexAndMapping() {

        //1.创建索引，在settings中自定义分析器whitespace_tokenizer_test，以空格来分词
        /**
        PUT whitespace_tokenizer_test
        {
            "settings": {
            "analysis": {
                "analyzer": {
                    "my_whitespace_tokenizer_analyzer": {
                        "type":      "custom",
                        "tokenizer": "whitespace"
                    }
                }
            }
        }
        }
         */
        try {
            CreateIndex.createIndexWithMapping(IndexName, "{\"settings\":{\"analysis\":{\"analyzer\":{\"my_whitespace_tokenizer_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"}}}}}");
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }


/**
   下面自定义一个复杂一点的分析器，然后进行测试
*/

    //自定义分析器 my_analyzer
    // 1.字符过滤器将"。"替换成"","&"替换成"and" 。注意char_filter中定义的mappings里面转换表达式里面要有前后的空格
    // 2.使用whitespace分词器
    // 3.定义停用词 "小","小明"
    /**
     * PUT my_analyzer_test
    {
        "settings": {
        "index": {
            "number_of_shards": "5",
            "number_of_replicas": "0",
            "analysis": {
                "filter": {
                    "my_stopwords_filter": {
                        "type": "stop",
                        "stopwords": ["小","小明"]
                    }
                },
                "char_filter": {
                    "symbol_to_filter": {
                        "mappings": [
                        "。=>  ",
                        "&=> and "
                        ],
                        "type": "mapping"
                    }
                },
                "analyzer": {
                    "my_analyzer": {
                        "filter": ["my_stopwords_filter"],
                        "char_filter": ["symbol_to_filter"],
                        "type": "custom",
                        "tokenizer": "whitespace"
                    }
                }
            }
        }
    }
    }
     */


    /** 自定义分析器my_analyzer测试
    GET my_analyzer_test/_analyze?analyzer=my_analyzer
    {
        "text": "小明 喜欢 小 红花,小红&小华 不喜欢 小红花。"
    }

    //返回结果如下：没有"小"，"小明"这两个token了，&被替换成了and,。号被去掉了

    {
        "tokens": [
        {
            "end_offset": 5,
                "start_offset": 3,
                "position": 1,
                "type": "word",
                "token": "喜欢"
        },
        {
            "end_offset": 10,
                "start_offset": 8,
                "position": 3,
                "type": "word",
                "token": "红花"
        },
        {
            "end_offset": 16,
                "start_offset": 11,
                "position": 104,
                "type": "word",
                "token": "小红and小华"
        },
        {
            "end_offset": 20,
                "start_offset": 17,
                "position": 105,
                "type": "word",
                "token": "不喜欢"
        },
        {
            "end_offset": 25,
                "start_offset": 21,
                "position": 106,
                "type": "word",
                "token": "小红花"
        }
        ]
    }
     */
}
