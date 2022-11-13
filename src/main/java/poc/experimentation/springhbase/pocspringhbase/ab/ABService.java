package poc.experimentation.springhbase.pocspringhbase.ab;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import poc.experimentation.springhbase.pocspringhbase.constants.HBaseConstants;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseColumn;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseConnection;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseRow;
import poc.experimentation.springhbase.pocspringhbase.request.BulkPutDto;
import poc.experimentation.springhbase.pocspringhbase.request.GetRowDto;
import poc.experimentation.springhbase.pocspringhbase.request.ScanTableDto;
import poc.experimentation.springhbase.pocspringhbase.service.HBaseCrudService;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ABService {

    @Autowired
    private HBaseConnection connection;

    @Autowired
    private HBaseCrudService hBaseCrudService;
    @Autowired
    private ObjectMapper mapper;

    private final byte[] columnFamily = Bytes.toBytes("data");
    private final byte[] au_qualifier = Bytes.toBytes("au");
    private final byte[] ua_qualifier = Bytes.toBytes("ua");
    private final byte[] country_qualifier = Bytes.toBytes("country");
    private final int scan_limit = 200;


    public boolean doesMappingExists(String audienceId, String entityId) throws IOException {
        String rowKey = HbaseUtils.getAudienceUserMapRowKey(audienceId, entityId);
        Result result = hBaseCrudService.getRow(new GetRowDto(HBaseConstants.DEFAULT_NAMESPACE, HBaseConstants.AUDIENCE_USER_MAP, rowKey));
        log.info(String.valueOf(result));
        return !(result == null || result.isEmpty());
    }

    public void createAudienceEntityMap(CreateAudienceEntityRequest request) throws IOException {

        if(request.getCountry() == null || !request.getCountry().equals("IN")){
            throw new RuntimeException("Invalid ISO country code");
        }
        List<AudienceEntityMap> audienceEntityMaps = new ArrayList<>();
        List<EntityAudienceMap> entityAudienceMaps = new ArrayList<>();

        for (String entityId : request.getEntityIds()) {

            if (entityId == null || entityId.equals("")) {
                continue;
            }

            audienceEntityMaps.add(AudienceEntityMap.builder()
                    .audienceEntityMapData(createAudienceEntityMapData(request.getAudienceId(), entityId, request.getEntityType()))
                    .country(CountryData.builder().isoCountryCode(request.getCountry()).build())
                    .build());

            entityAudienceMaps.add(EntityAudienceMap.builder()
                    .entityAudienceMapData(createEntityAudienceMapData(request.getAudienceId(), entityId, request.getEntityType()))
                    .country(CountryData.builder().isoCountryCode(request.getCountry()).build())
                    .build());
        }

        bulkHBaseAdd(audienceEntityMaps, entityAudienceMaps, request.getEntityType());
    }

    private void bulkHBaseAdd(List<AudienceEntityMap> audienceEntityMaps, List<EntityAudienceMap> entityAudienceMaps, String entityType) throws IOException {
//        bulkAddAudienceEntityMap(audienceEntityMaps, entityType);
        bulkAddAudienceEntityMapOp(audienceEntityMaps, entityType);
//        bulkAddEntityAudienceMap(entityAudienceMaps, entityType);
        bulkAddEntityAudienceMapOp(entityAudienceMaps, entityType);
    }

    private void bulkAddEntityAudienceMapOp(List<EntityAudienceMap> entityAudienceMaps, String entityType) throws JsonProcessingException {
        BulkPutDto dto = new BulkPutDto(HBaseConstants.DEFAULT_NAMESPACE, HBaseConstants.USER_AUDIENCE_MAP);
        for (EntityAudienceMap map : entityAudienceMaps) {
            HBaseRow row = new HBaseRow();
            if (entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)) {
                row.setRowKeyBytes(HbaseUtils.getUserAudienceRowKey(map.getEntityAudienceMapData().getAudienceId(), map.getEntityAudienceMapData().getUserId()));
            } else {
                row.setRowKeyBytes(HbaseUtils.getEntityAudienceRowKey(map.getEntityAudienceMapData().getAudienceId(), map.getEntityAudienceMapData().getEntityId(), entityType));
            }

            byte[] mapDataBytes = mapper.writeValueAsBytes(map.getEntityAudienceMapData());
            byte[] countryDataBytes = mapper.writeValueAsBytes(map.getCountry());

            row.getColumns().add(new HBaseColumn(this.columnFamily, this.ua_qualifier, mapDataBytes));
            row.getColumns().add(new HBaseColumn(this.columnFamily, this.country_qualifier, countryDataBytes));
            dto.getRows().add(row);
        }
        hBaseCrudService.bulkPut(dto);
    }

    private void bulkAddAudienceEntityMapOp(List<AudienceEntityMap> audienceEntityMaps, String entityType) throws JsonProcessingException {
        BulkPutDto dto = new BulkPutDto(HBaseConstants.DEFAULT_NAMESPACE, HBaseConstants.AUDIENCE_USER_MAP);

        for (AudienceEntityMap map : audienceEntityMaps) {
            HBaseRow row = new HBaseRow();
            if (entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)) {
                row.setRowKeyBytes(HbaseUtils.getAudienceEntityRowKey(map.getAudienceEntityMapData().getAudienceId().toString(), map.getAudienceEntityMapData().getUserId().toString()));
            } else {
                row.setRowKeyBytes(HbaseUtils.getAudienceEntityRowKey(map.getAudienceEntityMapData().getAudienceId().toString(), map.getAudienceEntityMapData().getEntityId()));
            }

            byte[] mapDataBytes = mapper.writeValueAsBytes(map.getAudienceEntityMapData());
            byte[] countryDataBytes = mapper.writeValueAsBytes(map.getCountry());

            row.getColumns().add(new HBaseColumn(this.columnFamily, this.au_qualifier, mapDataBytes));
            row.getColumns().add(new HBaseColumn(this.columnFamily, this.country_qualifier, countryDataBytes));
            dto.getRows().add(row);
        }
        
        hBaseCrudService.bulkPut(dto);
    }

//    private void bulkAddEntityAudienceMap(List<EntityAudienceMap> entityAudienceMaps, String entityType) throws IOException {
//        List<Put> putops = new ArrayList<>();
//        Table table = connection.getTable(HBaseConstants.DEFAULT_NAMESPACE, HBaseConstants.USER_AUDIENCE_MAP);
//        for (EntityAudienceMap map : entityAudienceMaps) {
//            byte[] rowKeyBytes;
//            if (entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)) {
//                rowKeyBytes = HbaseUtils.getUserAudienceRowKey(map.getEntityAudienceMapData().getAudienceId(), map.getEntityAudienceMapData().getUserId());
//            } else {
//                rowKeyBytes = HbaseUtils.getEntityAudienceRowKey(map.getEntityAudienceMapData().getAudienceId(), map.getEntityAudienceMapData().getEntityId(), entityType);
//            }
//
//            Put p = new Put(rowKeyBytes);
//            byte[] mapDataBytes = mapper.writeValueAsBytes(map.getEntityAudienceMapData());
//            byte[] countryDataBytes = mapper.writeValueAsBytes(map.getCountry());
//
//            p.addColumn(this.columnFamily, this.ua_qualifier, mapDataBytes);
//            p.addColumn(this.columnFamily, this.country_qualifier, countryDataBytes);
//            putops.add(p);
//        }
//        table.put(putops);
//    }

//    private void bulkAddAudienceEntityMap(List<AudienceEntityMap> audienceEntityMaps, String entityType) throws IOException {
//        List<Put> putops = new ArrayList<>();
//        Table table = connection.getTable(HBaseConstants.DEFAULT_NAMESPACE, HBaseConstants.AUDIENCE_USER_MAP);
//        for (AudienceEntityMap map : audienceEntityMaps) {
//            byte[] rowKeyBytes = null;
//            if (entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)) {
//                rowKeyBytes = HbaseUtils.getAudienceEntityRowKey(map.getAudienceEntityMapData().getAudienceId().toString(), map.getAudienceEntityMapData().getUserId().toString());
//            } else {
//                rowKeyBytes = HbaseUtils.getAudienceEntityRowKey(map.getAudienceEntityMapData().getAudienceId().toString(), map.getAudienceEntityMapData().getEntityId());
//            }
//
//            Put p = new Put(rowKeyBytes);
//            byte[] mapDataBytes = mapper.writeValueAsBytes(map.getAudienceEntityMapData());
//            byte[] countryDataBytes = mapper.writeValueAsBytes(map.getCountry());
//            p.addColumn(this.columnFamily, this.au_qualifier, mapDataBytes);
//            p.addColumn(this.columnFamily, this.country_qualifier, countryDataBytes);
//            putops.add(p);
//        }
//        table.put(putops);
//    }

    private EntityAudienceMapData createEntityAudienceMapData(Integer audienceId, String entityId, String entityType) {
        if (entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)) {
            return EntityAudienceMapData.builder().audienceId(audienceId).userId(Long.parseLong(entityId)).build();
        } else {
            return EntityAudienceMapData.builder().audienceId(audienceId).entityId(entityId).build();
        }
    }

    private AudienceEntityMapData createAudienceEntityMapData(Integer audienceId, String entityId, String entityType) {
        AudienceEntityMapData audienceEntityMapData = new AudienceEntityMapData();
        audienceEntityMapData.setAudienceId(audienceId);
        if (entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)) {
            audienceEntityMapData.setUserId(Long.parseLong(entityId));
        } else {
            audienceEntityMapData.setEntityId(entityId);
        }
        return audienceEntityMapData;
    }

    public List<String> getAudiencesForEntity(String entityId, String entityType) throws IOException {
        String pattern = (entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)) ? entityId : (entityType + "_" + entityId);
        String prefixRowKey = String.format("%s_", HbaseUtils.getSha1HexString(pattern));
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(prefixRowKey + "*"));
        Set<String> audienceSet = new HashSet<>();
        String[] lastRowKey = {null};
        boolean includeStartRow = true;
        while (true) {
            byte[] startRowKey;
            if (lastRowKey[0] != null) {
                startRowKey = lastRowKey[0].getBytes();
                includeStartRow = false;
            } else {
                startRowKey = prefixRowKey.getBytes();
                includeStartRow = true;
            }
            byte[] stopRowBytes = HbaseUtils.calculateTheClosestNextRowKeyForPrefix(startRowKey);

            ScanTableDto scanTableDto = ScanTableDto.builder()
                    .namespace(HBaseConstants.DEFAULT_NAMESPACE)
                    .tableName(HBaseConstants.USER_AUDIENCE_MAP)
                    .limit(this.scan_limit)
                    .startRow(startRowKey)
                    .endRow(stopRowBytes)
                    .filter(filter)
                    .includeStartRow(includeStartRow).build();

            List<Result> resultList = hBaseCrudService.scanTable(scanTableDto);

            if (resultList == null || resultList.isEmpty()) {
                break;
            }

            List<EntityAudienceMapData> entityAudienceMapDataList = resultList.stream().map(r -> {
                byte[] value = r.getValue(this.columnFamily, this.ua_qualifier);
                try {
                    return this.mapper.readValue(value, EntityAudienceMapData.class);
                } catch (IOException e) {
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toList());

            lastRowKey[0] = HbaseUtils.getLastRowKey(entityAudienceMapDataList, entityType);
            List<String> audienceIdList = entityAudienceMapDataList.stream()
                    .map(ea -> ea.getAudienceId())
                    .filter(Objects::nonNull)
                    .map(String::valueOf).collect(Collectors.toList());
            audienceSet.addAll(audienceIdList);
        }

        return new ArrayList<>(audienceSet);

    }

    public List<String> getEntitiesForAudience(Integer audienceId, String entityType) throws IOException {
        String prefixKey = String.format("%s_", HbaseUtils.getSha1HexString(audienceId.toString()));
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(prefixKey + "*"));
        String[] lastRowKey = {null};
        Set<String> entitySet = new HashSet<>();
        boolean includeStartRow = true;
        while (true) {
            if (lastRowKey[0] != null) {
                includeStartRow = false;
                prefixKey = lastRowKey[0];
            }
            byte[] stopRowBytes = HbaseUtils.calculateTheClosestNextRowKeyForPrefix(Bytes.toBytes(prefixKey));

            ScanTableDto scanTableDto = ScanTableDto.builder()
                    .namespace(HBaseConstants.DEFAULT_NAMESPACE)
                    .tableName(HBaseConstants.AUDIENCE_USER_MAP)
                    .limit(this.scan_limit)
                    .startRow(prefixKey.getBytes())
                    .endRow(stopRowBytes)
                    .filter(filter)
                    .includeStartRow(includeStartRow).build();

            List<Result> resultList = hBaseCrudService.scanTable(scanTableDto);
            if (resultList == null || resultList.isEmpty()) {
                break;
            }
            List<AudienceEntityMapData> audienceEntityMapDataList = resultList.stream().map(r -> {
                byte[] value = r.getValue(this.columnFamily, this.au_qualifier);
                try {
                    return this.mapper.readValue(value, AudienceEntityMapData.class);
                } catch (IOException e) {
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toList());

            lastRowKey[0] = HbaseUtils.getAudienceEntityLastRowKey(audienceEntityMapDataList, entityType);
            List<String> entityIds = audienceEntityMapDataList.stream().map(ae -> {
                if (entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)) {
                    return ae.getUserId().toString();
                } else {
                    return ae.getEntityId();
                }
            }).filter(Objects::nonNull).collect(Collectors.toList());
            entitySet.addAll(entityIds);
        }

        return new ArrayList<>(entitySet);
    }
}
