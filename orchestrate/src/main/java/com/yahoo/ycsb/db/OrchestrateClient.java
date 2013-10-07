package com.yahoo.ycsb.db;

import com.ning.http.client.*;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

public class OrchestrateClient extends DB {
    private String apiKey;
    private String baseUrl = "https://api.orchestrate.io/v0/";
    private AsyncHttpClient client;
    private Realm realm;

    public void init() throws DBException
    {
        apiKey = getProperties().getProperty("apiKey");
        if (apiKey == null)
        {
            System.err.println("Error, must specify a apiKey for Orchestrate.io App");
            throw new DBException("No apiKey specified");
        }

        if (getProperties().getProperty("baseUrl")!=null) {
            baseUrl = getProperties().getProperty("baseUrl");
        }

        client = new AsyncHttpClient();

        realm = new Realm.RealmBuilder()
                .setPrincipal(apiKey)
                .setPassword("")
                .setUsePreemptiveAuth(true)
                .setScheme(Realm.AuthScheme.BASIC)
                .build();
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        String url = new StringBuilder().append(baseUrl).append(table).append('/').append(key).toString();
        ObjectMapper mapper = new ObjectMapper();

        try {
            Response response = client.prepareGet(url).setRealm(realm).execute().get();

            if (response.getStatusCode() != 200) return 2;

            Map<String, String> map = mapper.readValue(response.getResponseBodyAsStream(), new TypeReference<Map<String, String>>() { });

            if (fields != null) {
                for (String field : fields) {
                    result.put(field, new StringByteIterator(map.get(field)));
                }
            } else result.putAll(StringByteIterator.getByteIteratorMap(map));
        } catch (InterruptedException e) {
            e.printStackTrace();
            return 2;
        } catch (ExecutionException e) {
            e.printStackTrace();
            return 2;
        } catch (IOException e) {
            e.printStackTrace();
            return 2;
        }

        return 0;
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        String url = new StringBuilder().append(baseUrl).append(table).append('/').append(key).toString();

        ObjectMapper mapper = new ObjectMapper();

        try {
            byte[] body = mapper.writeValueAsBytes(StringByteIterator.getStringMap(values));

            Response response = client.preparePut(url)
                    .setRealm(realm)
                    .setHeader("Content-Type", "application/json")
                    .setBody(body)
                    .execute()
                    .get();

            if (response.getStatusCode() != 201) {
                System.out.println(response.getStatusCode());
                return 2;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return 2;
        } catch (ExecutionException e) {
            e.printStackTrace();
            return 2;
        } catch (IOException e) {
            e.printStackTrace();
            return 2;
        }

        return 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        return update(table, key, values);
    }

    @Override
    public int delete(String table, String key) {
        return 0;
    }
}
