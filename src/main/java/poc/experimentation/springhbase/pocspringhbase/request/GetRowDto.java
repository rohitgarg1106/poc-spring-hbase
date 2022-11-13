package poc.experimentation.springhbase.pocspringhbase.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetRowDto {
    private String namespace;
    private String tableName;
    private String row;
}
