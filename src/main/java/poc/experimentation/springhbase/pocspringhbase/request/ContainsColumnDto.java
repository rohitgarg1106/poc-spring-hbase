package poc.experimentation.springhbase.pocspringhbase.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContainsColumnDto {

    private String tableName;
    private String namespace;
    private String row;
    private String columnFamily;
    private String columnQualifier;

}
