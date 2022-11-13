package poc.experimentation.springhbase.pocspringhbase.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScanTableRequest {

    private String namespace;
    private String tableName;
    private Integer limit;
    private String regexPattern;
    private String columnFamily;
    private String columnQualifier;
}
