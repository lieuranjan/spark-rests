package in.gogoi.spark.rest.ds;

import in.gogoi.spark.rest.ds.config.ApiDsConfig;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

import java.io.Serializable;
import java.util.HashMap;

@Log4j2
public class ApiDefaultDataSource implements RelationProvider, SchemaRelationProvider, Serializable {


    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, final Map<String, String> parameters) {
        return createRelation(sqlContext, parameters, null);
    }

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, final Map<String, String> parameters, final StructType schema) {
        java.util.Map<String, String> map = null;
        if (parameters != null) {
            map = new HashMap<>();
            map.putAll(JavaConverters.mapAsJavaMapConverter(parameters).asJava());
        }
        val apiConfig = new ApiDsConfig(map);
        return new ApiRelation(apiConfig, sqlContext, schema);
    }
}
