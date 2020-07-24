package in.gogoi.spark.rest.ds.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.List;

public class JsonDataUtils {

    public static List<String> extractJsonRecords(String jsonstr) {
        if (jsonstr.isEmpty()) {
            return new ArrayList<>();
        }
        JsonParser parser = new JsonParser();
        JsonElement root = parser.parse(jsonstr);
        List<String> array = new ArrayList<>();
        if (root.isJsonArray()) {
            int size = root.getAsJsonArray().size();
            JsonArray jsonArray = root.getAsJsonArray();
            for (int i = 0; i < size; i++) {
                String jsnout = jsonArray.get(i).getAsJsonObject().toString();
                array.add(jsnout);
            }
        } else {
            String jsnout = root.getAsJsonObject().toString();
            array.add(jsnout);
        }
        return array;
    }
}
