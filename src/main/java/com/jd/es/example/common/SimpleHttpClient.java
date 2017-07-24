package com.jd.es.example.common;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Title: SimpleHttpClient
 * Description: SimpleHttpClient
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/8/5
 *
 * @author <a href=mailto:zhouzhichao@jd.com>wutao,chaochao</a>
 */
public class SimpleHttpClient {

    public static String ContentType_Json = "application/json";

    public static String get(String urlStr, String requestMethod) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        if (requestMethod != null && !"".equals(requestMethod)) {
            connection.setRequestMethod(requestMethod);
        }
        connection.connect();
        InputStream in = null;
        if (connection.getResponseCode() >= 400) {
            in = connection.getErrorStream();
        } else {
            in = connection.getInputStream();
        }
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
        StringBuilder content = new StringBuilder();
        String tmp;
        while ((tmp = bufferedReader.readLine()) != null) {
            content.append(tmp);
        }
        if (in != null) {
            in.close();
        }
        bufferedReader.close();
        connection.disconnect();
        return content.toString();
    }

    public static String delete(String urlStr, String content) throws IOException {
        return request(urlStr, content, "DELETE", ContentType_Json);
    }

    public static String post(String urlStr, String content) throws IOException {
        return request(urlStr, content, "POST", ContentType_Json);
    }

    public static String put(String urlStr, String content) throws IOException {
        return request(urlStr, content, "PUT", ContentType_Json);
    }

    public static String request(String urlStr, String content, String requestMethod, String contentType) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod(requestMethod);
        connection.setUseCaches(false);
        connection.setConnectTimeout(1000 * 5);//5秒
        connection.setReadTimeout(1000 * 60);//1分钟
        connection.setInstanceFollowRedirects(true);
        connection.setRequestProperty("Content-type", contentType);
        connection.connect();
        Writer writer = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));
        if (content != null && !"".equals(content)) {
            writer.write(content);
        }
        writer.flush();
        writer.close();
        InputStream in = null;
        if (connection.getResponseCode() >= 400) {
            in = connection.getErrorStream();
        } else {
            in = connection.getInputStream();
        }
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
        StringBuilder response = new StringBuilder();
        String tmp;
        while ((tmp = bufferedReader.readLine()) != null) {
            response.append(tmp);
        }
        if (in != null) {
            in.close();
        }
        bufferedReader.close();
        connection.disconnect();
        return response.toString();
    }
}