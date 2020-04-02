package com;


import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient(){
        //https://e7saak0bew:mwz4xvj9hr@app-kafka-testing-6056174084.eu-west-1.bonsaisearch.net:443
        String hostname = "app-kafka-testing-6056174084.eu-west-1.bonsaisearch.net";
        String username = "e7saak0bew";
        String password = "mwz4xvj9hr";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {
        Logger logger= LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client=createClient();
        String jsonString="{\"hi\":\"bar\"}";
        IndexRequest indexRequest=new IndexRequest("twitter","tweets","1").source(jsonString, XContentType.JSON);
        IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
        String id=indexResponse.getId();
        logger.info(id);
        client.close();
    }
}

