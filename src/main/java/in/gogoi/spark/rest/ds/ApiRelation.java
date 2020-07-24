package in.gogoi.spark.rest.ds;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import in.gogoi.spark.rest.ds.config.ApiDsConfig;
import in.gogoi.spark.rest.ds.config.DataFormat;
import in.gogoi.spark.rest.ds.utils.JsonDataUtils;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.http.HttpHeaders;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Log4j2
public class ApiRelation extends BaseRelation implements TableScan, Serializable {

    private SQLContext sqlContext;
    private StructType schema;
    private Map<String, String> headers;
    private ApiDsConfig apiConfig;
    private ApiClient apiDatasource;

    public ApiRelation(final ApiDsConfig apiConfig, final SQLContext sqlContext, final StructType schema) {
        this.sqlContext = sqlContext;
        this.schema = schema;
        this.apiConfig = apiConfig;
        this.headers = new HashMap<>();
        headers.put(HttpHeaders.CONTENT_TYPE, apiConfig.getContentType());
        this.apiDatasource = new ApiClient(
                apiConfig.getConnectionTimeout(),
                apiConfig.getReadTimeout(),
                apiConfig.getCharset(),
                headers
        );
    }

    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    @Override
    public StructType schema() {
        if (Objects.isNull(schema)) {
            return new StructType(new StructField[]{
                    new StructField("line", DataTypes.StringType, false, Metadata.empty())
            });
        } else {
            return schema;
        }
    }

    @Override
    public RDD<Row> buildScan() {
        String url = apiConfig.getUrl();
        if (!apiConfig.getAuthUrl().isEmpty()) {
            //get token
            String authUrl = apiConfig.getAuthUrl();
            String token = apiDatasource.getGETMethodData(authUrl);
            url = url.replaceAll("\\$\\{token\\}", token);
        }
        //get data
        //TODO
        List<String> dataUrls = getFormattedUrl(url, apiConfig.getUrlParams());
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sqlContext.sparkSession().sparkContext());
        JavaRDD<String> javaRDD = javaSparkContext.parallelize(dataUrls).flatMap(l -> getLinesFromUrl(l).iterator());
        return javaRDD.map(r -> RowFactory.create(r)).rdd();
    }

    protected List<String> getFormattedUrl(String url, final String urlParams) {

        val urls = new ArrayList<String>();
        if (!urlParams.isEmpty()) {
            val params = urlParams.split("#", -1);
            for (val kv : params) {
                val keyValues = kv.split("=", -1);
                try {
                    url = url.replaceAll("\\$\\{" + keyValues[0] + "\\}", URLEncoder.encode(keyValues[1], StandardCharsets.UTF_8.name()));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("Error preparing url from params", e);
                }
            }
        }
        urls.add(url);
        return urls;
    }

    private List<String> getLinesFromUrl(final String url) {
        val output = new ArrayList<String>();
        val apiResponse = callAPI(url);
        if (apiConfig.getDataFormat() == DataFormat.DELIMITED) {
            val lines = apiResponse.split(apiConfig.getLineSeparator());
            output.addAll(Arrays.stream(lines).collect(Collectors.toList()));
        } else if (apiConfig.getDataFormat() == DataFormat.JSON) {
            String jsonStr = apiResponse;
            if (!apiConfig.getJsonRootElement().isEmpty()) {
                val jsonObj = Configuration.defaultConfiguration().jsonProvider().parse(apiResponse);
                val objValue = JsonPath.read(jsonObj, apiConfig.getJsonRootElement());
                jsonStr = objValue == null ? "" : objValue.toString();
            }
            output.addAll(JsonDataUtils.extractJsonRecords(jsonStr));
        } else {
            throw new RuntimeException("Datatype Not Suppoted yet");
        }
        return output;
    }

    private String callAPI(final String url) {
        val methodType = apiConfig.getMethod();
        switch (methodType) {
            case GET:
                return apiDatasource.getGETMethodData(url);
            case POST:
                return apiDatasource.getPostMethodData(url, apiConfig.getParamsJson());
            default:
                throw new RuntimeException(methodType + " MethodType not supported yet");
        }
    }
}
