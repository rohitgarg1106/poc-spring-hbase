package poc.experimentation.springhbase.pocspringhbase.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HBaseData {

    private String namespace;


    private String tableName;


    private String row;


    private String columnFamily;


    private String columnQualifier;


    private Object data;

    private long timestamp;

    public HBaseData(String namespace, String tableName, String row, String columnFamily, String columnQualifier, Map data) {

        this.namespace = namespace;
        this.tableName = tableName;
        this.row = row;
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.data = data;
    }
}
