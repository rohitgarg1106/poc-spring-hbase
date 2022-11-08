package poc.experimentation.springhbase.pocspringhbase.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseData;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateMapStoreRequest {

    @JsonProperty("namespace")
    private String namespace;

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("column_families")
    private List<String> cf;

}
