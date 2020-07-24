package in.gogoi.spark.rest.ds;

import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

@Log4j2
public class ApiClient implements Serializable {
    private int connectionTimeout;
    private int readTimeout;
    private String charsets;
    private Map<String, String> headers;

    public ApiClient(int connectionTimeout, int readTimeout, String charsets, Map<String, String> headers) {
        this.connectionTimeout = connectionTimeout;
        this.readTimeout = readTimeout;
        this.charsets = charsets;
        this.headers = headers;
    }

    private void addHeaders(final HttpUriRequest client) {
        if (headers != null && !headers.isEmpty()) {
            val keys = headers.keySet();
            for (String key : keys) {
                String value = headers.getOrDefault(key, "");
                if (!value.isEmpty()) {
                    client.addHeader(key, value);

                }
            }
        }
    }

    /**
     * @param url
     * @return
     */
    public String getGETMethodData(final String url) {
        val requet = new HttpGet(url);
        addHeaders(requet);
        return getAPIContent(requet);
    }

    /**
     * @param url
     * @param body
     * @return
     */
    public String getPostMethodData(final String url, final String body) {
        val request = new HttpPost(url);
        addHeaders(request);
        if (!body.isEmpty()) {
            try {
                request.setEntity(new StringEntity(body));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        return getAPIContent(request);
    }

    /**
     * @param request
     * @return
     */
    private String getAPIContent(final HttpUriRequest request) {
        HttpResponse httpResponse = null;
        String content = "";
        try {
            val sslContext = SSLContextBuilder.create().loadTrustMaterial(null, (x509Certificates, s) -> true).build();
            val socketFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
            val requestConfig = RequestConfig.custom()
                    .setConnectTimeout(connectionTimeout)
                    .setConnectionRequestTimeout(readTimeout)
                    .build();
            val httpClientBuilder = HttpClientBuilder.create();
            httpClientBuilder.setDefaultRequestConfig(requestConfig);
            httpClientBuilder.setSSLSocketFactory(socketFactory);
            val useSysProxy = Boolean.valueOf(System.getProperty("java.net.useSystemProxy", "false"));
            if (useSysProxy) {
                String host = System.getProperty("http.proxyHosr", "");
                int port = Integer.valueOf(System.getProperty("http.proxyPort", "8080"));
                String user = System.getProperty("http.proxyUser", "");
                String password = System.getProperty("http.proxyPassword", "");
                HttpHost proxy = new HttpHost(host, port);
                Credentials credentials = new UsernamePasswordCredentials(user, password);
                AuthScope authScope = new AuthScope(host, port);
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(authScope, credentials);
                httpClientBuilder.setProxy(proxy);
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
            val httpClient = httpClientBuilder.build();
            httpResponse = httpClient.execute(request);
            if (httpResponse.getStatusLine().getStatusCode() == 200) {
                val entity = httpResponse.getEntity();
                content = IOUtils.toString(entity.getContent(), charsets);
            } else {
                throw new RuntimeException("API Response Failed," + httpResponse);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (KeyStoreException e) {
            throw new RuntimeException(e);
        } catch (KeyManagementException e) {
            throw new RuntimeException(e);
        }
        return content;
    }
}
