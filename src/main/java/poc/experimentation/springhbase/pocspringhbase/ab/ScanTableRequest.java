package poc.experimentation.springhbase.pocspringhbase.ab;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.web.bind.annotation.RequestParam;

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
