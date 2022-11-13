package poc.experimentation.springhbase.pocspringhbase.ab;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import poc.experimentation.springhbase.pocspringhbase.constants.HBaseConstants;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseConnection;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ABService {

    @Autowired
    private HBaseConnection connection;

    @Autowired
    private ObjectMapper mapper;

    private final byte[] columnFamily = Bytes.toBytes("data");
    private final byte[] au_qualifier = Bytes.toBytes("au");
    private final byte[] ua_qualifier = Bytes.toBytes("ua");
    private final byte[] country_qualifier = Bytes.toBytes("country");
    private final int scan_limit = 200;


    public boolean doesMappingExists(String audienceId, String entityId) throws IOException {
        String rowKey = HbaseUtils.getAudienceUserMapRowKey(audienceId, entityId);
        Result result = connection.getRow(HBaseConstants.DEFAULT_NAMESPACE, HBaseConstants.AUDIENCE_USER_MAP, rowKey);
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
        bulkAddAudienceEntityMap(audienceEntityMaps, entityType);
        bulkAddEntityAudienceMap(entityAudienceMaps, entityType);
    }

    private void bulkAddEntityAudienceMap(List<EntityAudienceMap> entityAudienceMaps, String entityType) throws IOException {
        List<Put> putops = new ArrayList<>();
        Table table = connection.getTable(HBaseConstants.DEFAULT_NAMESPACE, HBaseConstants.USER_AUDIENCE_MAP);
        for (EntityAudienceMap map : entityAudienceMaps) {
            byte[] rowKeyBytes;
            if (entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)) {
                rowKeyBytes = HbaseUtils.getUserAudienceRowKey(map.getEntityAudienceMapData().getAudienceId(), map.getEntityAudienceMapData().getUserId());
            } else {
                rowKeyBytes = HbaseUtils.getEntityAudienceRowKey(map.getEntityAudienceMapData().getAudienceId(), map.getEntityAudienceMapData().getEntityId(), entityType);
            }

            Put p = new Put(rowKeyBytes);
            byte[] mapDataBytes = mapper.writeValueAsBytes(map.getEntityAudienceMapData());
            byte[] countryDataBytes = mapper.writeValueAsBytes(map.getCountry());

            p.addColumn(this.columnFamily, this.ua_qualifier, mapDataBytes);
            p.addColumn(this.columnFamily, this.country_qualifier, countryDataBytes);
            putops.add(p);
        }
        table.put(putops);
    }

    private void bulkAddAudienceEntityMap(List<AudienceEntityMap> audienceEntityMaps, String entityType) throws IOException {
        List<Put> putops = new ArrayList<>();
        Table table = connection.getTable(HBaseConstants.DEFAULT_NAMESPACE, HBaseConstants.AUDIENCE_USER_MAP);
        for (AudienceEntityMap map : audienceEntityMaps) {
            byte[] rowKeyBytes = null;
            if (entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)) {
                rowKeyBytes = HbaseUtils.getAudienceEntityRowKey(map.getAudienceEntityMapData().getAudienceId().toString(), map.getAudienceEntityMapData().getUserId().toString());
            } else {
                rowKeyBytes = HbaseUtils.getAudienceEntityRowKey(map.getAudienceEntityMapData().getAudienceId().toString(), map.getAudienceEntityMapData().getEntityId());
            }

            Put p = new Put(rowKeyBytes);
            byte[] mapDataBytes = mapper.writeValueAsBytes(map.getAudienceEntityMapData());
            byte[] countryDataBytes = mapper.writeValueAsBytes(map.getCountry());
            p.addColumn(this.columnFamily, this.au_qualifier, mapDataBytes);
            p.addColumn(this.columnFamily, this.country_qualifier, countryDataBytes);
            putops.add(p);
        }
        table.put(putops);
    }

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

//    public List<String> getAudiencesForEntityOld(String entityId, String entityType) throws IOException {
//        String pattern = (entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)) ? entityId : (entityType + "_" + entityId);
//        String prefixRowKey = String.format("%s_", HbaseUtils.getSha1HexString(pattern));
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(prefixRowKey + "*"));
//        Set<String> audienceSet = new HashSet<>();
//        String[] lastRowKey = {null};
//        boolean includeStartRow = true;
//        while (true) {
//            String startRowKey;
//            if (lastRowKey[0] != null) {
//                startRowKey = lastRowKey[0];
//                includeStartRow = false;
//            } else {
//                startRowKey = prefixRowKey;
//                includeStartRow = true;
//            }
//            byte[] stopRowBytes = HbaseUtils.calculateTheClosestNextRowKeyForPrefix(Bytes.toBytes(startRowKey));
//            List<Result> resultList = connection.scanTable(HBaseConstants.DEFAULT_NAMESPACE, HBaseConstants.USER_AUDIENCE_MAP, this.scan_limit, startRowKey, stopRowBytes.toString(), filter, includeStartRow);
//
//            if (resultList == null || resultList.isEmpty()) {
//                break;
//            }
//
//            List<EntityAudienceMapData> entityAudienceMapDataList = resultList.stream().map(r -> {
//                byte[] value = r.getValue(this.columnFamily, this.ua_qualifier);
//                try {
//                    return this.mapper.readValue(value, EntityAudienceMapData.class);
//                } catch (IOException e) {
//                    return null;
//                }
//            }).filter(Objects::nonNull).collect(Collectors.toList());
//
//            lastRowKey[0] = HbaseUtils.getLastRowKeyOld(entityAudienceMapDataList, entityType);
//            List<String> audienceIdList = entityAudienceMapDataList.stream()
//                    .map(ea -> ea.getAudienceId())
//                    .filter(Objects::nonNull)
//                    .map(String::valueOf).collect(Collectors.toList());
//            audienceSet.addAll(audienceIdList);
//        }
//
//        return new ArrayList<>(audienceSet);
//
//    }

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
            List<Result> resultList = connection.scanTable(HBaseConstants.DEFAULT_NAMESPACE, HBaseConstants.USER_AUDIENCE_MAP, this.scan_limit, startRowKey, stopRowBytes, filter, includeStartRow);

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
        String prefixKey = String.format("%s_*", HbaseUtils.getSha1HexString(audienceId.toString()));
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(prefixKey));
        String[] lastRowKey = {null};
        Set<String> entitySet = new HashSet<>();
        boolean includeStartRow = true;
        while (true) {
            if (lastRowKey[0] != null) {
                includeStartRow = false;
                prefixKey = lastRowKey[0];
            }
            byte[] stopRowBytes = HbaseUtils.calculateTheClosestNextRowKeyForPrefix(Bytes.toBytes(prefixKey));
            List<Result> resultList = connection.scanTable(HBaseConstants.DEFAULT_NAMESPACE, HBaseConstants.AUDIENCE_USER_MAP, this.scan_limit, prefixKey, stopRowBytes.toString(), filter, includeStartRow);
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
