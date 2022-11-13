package poc.experimentation.springhbase.pocspringhbase.ab;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CountryData {
    private String isoCountryCode;
}
