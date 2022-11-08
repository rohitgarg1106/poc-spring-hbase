package poc.experimentation.springhbase.pocspringhbase.request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseData;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GetDataRequest {

    @JsonProperty("namespace")
    private String namespace;

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("row")
    private String row;

    @JsonProperty("column_family")
    private String columnFamily;

    @JsonProperty("colum_qualifier")
    private String columnQualifier;

    @JsonIgnore
    public HBaseData getHBaseData(){
        HBaseData hdata = new HBaseData();
        hdata.setNamespace(this.namespace);
        hdata.setTableName(this.tableName);
        hdata.setRow(this.row);
        hdata.setColumnFamily(this.columnFamily);
        hdata.setColumnQualifier(this.columnQualifier);
        return hdata;
    }
}
