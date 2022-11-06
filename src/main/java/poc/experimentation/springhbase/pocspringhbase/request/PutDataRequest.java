package poc.experimentation.springhbase.pocspringhbase.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PutDataRequest {

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

    @JsonProperty("data")
    private Map<?,?> data;
}
