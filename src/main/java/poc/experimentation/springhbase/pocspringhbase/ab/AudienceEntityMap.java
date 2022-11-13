package poc.experimentation.springhbase.pocspringhbase.ab;

import lombok.Builder;
import lombok.Data;
@Data
@Builder
public class AudienceEntityMap {

    private AudienceEntityMapData audienceEntityMapData;
    private CountryData country;

}
