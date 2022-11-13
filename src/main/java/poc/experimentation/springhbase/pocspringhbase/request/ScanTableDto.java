package poc.experimentation.springhbase.pocspringhbase.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.filter.Filter;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ScanTableDto {

    private String namespace;
    private String tableName;
    private Integer limit;
    private byte[] startRow;
    private byte[] endRow;
    private Filter filter;
    private boolean includeStartRow;
}
