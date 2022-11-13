package poc.experimentation.springhbase.pocspringhbase.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class HBaseColumn {
    byte[] columnFamily;
    byte[] columnQualifier;
    byte[] data;
}
