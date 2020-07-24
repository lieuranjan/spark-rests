package in.gogoi.spark.rest.ds.config;

import lombok.Data;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Log4j2
@Data
public class ApiDsConfig implements Serializable {

    private String dataFormat;
    private String lineSeparator;
    private String urlParams;
    private String method;
    private String charset;
    private String authUrl;
    private String url;
    private String connectionTimeout;
    private String readTimeout;
    private String jsonRootElement;
    private String hasHeader;
    private String headerCount;
    private String inputBodyFormat;
    private String paramsJson;
    private String contentType;

    public ApiDsConfig(final Map<String, String> options) {
        this();
        this.addConfigs(options);
    }

    private void put(final String key, final String value) {
        try {
            val field = this.getClass().getDeclaredField(key);
            field.set(this, value);
        } catch (Exception e) {
            //log.error("Could  not find key {} , Ignoring this property",key,e);
        }
    }

    private void addConfigs(Map<String, String> options) {
        for (Map.Entry<String, String> config : options.entrySet()) {
            this.put(config.getKey(), config.getValue());
        }
    }


    public String getLineSeparator() {
        return StringUtils.isBlank(lineSeparator) ? "\n" : lineSeparator;
    }

    public String getCharset() {
        return StringUtils.isBlank(charset) ? StandardCharsets.UTF_8.name() : Charset.forName(charset.trim()).name();
    }


    public String getUrl() {
        if (StringUtils.isBlank(url)) {
            throw new RuntimeException("Url should not be null");
        }
        return url.trim();
    }

    public DataFormat getDataFormat() {
        if (StringUtils.isBlank(dataFormat)) {
            return DataFormat.JSON;
        }
        return DataFormat.valueOf(dataFormat.trim());
    }

    public HttpMethod getMethod() {
        if (StringUtils.isBlank(method)) {
            return HttpMethod.GET;
        }
        return HttpMethod.valueOf(method.trim());
    }

    public Integer getConnectionTimeout() {
        if (StringUtils.isBlank(connectionTimeout)) {
            return 6000;
        }
        return Integer.parseInt(connectionTimeout);
    }

    public Integer getReadTimeout() {
        if (StringUtils.isBlank(readTimeout)) {
            return 6000;
        }
        return Integer.parseInt(readTimeout);
    }

    public boolean getHasHeader() {
        if (StringUtils.isBlank(hasHeader)) {
            return false;
        }
        return Boolean.parseBoolean(hasHeader);
    }

    public Integer getHeaderCount() {
        if (StringUtils.isBlank(headerCount)) {
            return 1;
        }
        return Integer.parseInt(headerCount);
    }

    public String getContentType() {
        switch (getDataFormat()) {
            case DELIMITED:
                return "text/plain";
            case JSON:
                return "application/json";
            default:
                throw new RuntimeException(dataFormat + " Datatype not supported yet");
        }
    }


}
