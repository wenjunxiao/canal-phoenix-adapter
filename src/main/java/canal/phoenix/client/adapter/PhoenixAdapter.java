package canal.phoenix.client.adapter;

import canal.phoenix.client.adapter.config.ConfigLoader;
import canal.phoenix.client.adapter.config.MappingConfig;
import canal.phoenix.client.adapter.monitor.PhoenixConfigMonitor;
import canal.phoenix.client.adapter.service.PhoenixEtlService;
import canal.phoenix.client.adapter.service.PhoenixSyncService;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import canal.phoenix.client.adapter.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Phoenix适配器实现类
 */
@SPI("phoenix")
public class PhoenixAdapter implements OuterAdapter {

    private static Logger logger = LoggerFactory.getLogger(PhoenixAdapter.class);

    private Map<String, MappingConfig> phoenixMapping = new ConcurrentHashMap<>();                // 文件名对应配置
    private Map<String, Map<String, MappingConfig>> mappingConfigCache = new ConcurrentHashMap<>();                // 库名-表名对应配置

    private DruidDataSource dataSource;

    private PhoenixSyncService phoenixSyncService;

    private PhoenixConfigMonitor phoenixConfigMonitor;

    private Properties envProperties;

    public Map<String, MappingConfig> getPhoenixMapping() {
        return phoenixMapping;
    }

    public Map<String, Map<String, MappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }

    public PhoenixAdapter() {
        logger.info("PhoenixAdapter create: {} {}", this, Thread.currentThread().getStackTrace());
    }
    /**
     * 初始化方法
     *
     * @param configuration 外部适配器配置信息
     */
    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        this.envProperties = envProperties;
        Map<String, MappingConfig> phoenixMappingTmp = ConfigLoader.load(envProperties);
        // 过滤不匹配的key的配置
        phoenixMappingTmp.forEach((key, mappingConfig) -> {
            if ((mappingConfig.getOuterAdapterKey() == null && configuration.getKey() == null)
                    || (mappingConfig.getOuterAdapterKey() != null
                    && mappingConfig.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey()))) {
                phoenixMapping.put(key, mappingConfig);
            }
        });

        if (phoenixMapping.isEmpty()) {
            throw new RuntimeException("No phoenix adapter found for config key: " + configuration.getKey());
        } else {
            logger.info("[{}]phoenix config mapping: {}", this, phoenixMapping.keySet());
        }

        for (Map.Entry<String, MappingConfig> entry : phoenixMapping.entrySet()) {
            String configName = entry.getKey();
            MappingConfig mappingConfig = entry.getValue();
            String key;
            if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-"
                        + StringUtils.trimToEmpty(mappingConfig.getGroupId()) + "_"
                        + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable().toLowerCase();
            } else {
                key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_"
                        + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable().toLowerCase();
            }
            Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(key,
                    k1 -> new ConcurrentHashMap<>());
            configMap.put(configName, mappingConfig);
        }

        // 初始化连接池
        Map<String, String> properties = configuration.getProperties();
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(properties.get("jdbc.driverClassName"));
        dataSource.setUrl(properties.get("jdbc.url"));
        dataSource.setUsername(properties.get("jdbc.username"));
        dataSource.setPassword(properties.get("jdbc.password"));
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(30);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setUseUnfairLock(true);
        // List<String> array = new ArrayList<>();
        // array.add("set names utf8mb4;");
        // dataSource.setConnectionInitSqls(array);

        try {
            dataSource.init();
        } catch (SQLException e) {
            logger.error("ERROR ## failed to initial datasource: " + properties.get("jdbc.url"), e);
        }

        String threads = properties.get("threads");
        // String commitSize = properties.get("commitSize");

        phoenixSyncService = new PhoenixSyncService(dataSource,
                threads != null ? Integer.valueOf(threads) : null
        );

        phoenixConfigMonitor = new PhoenixConfigMonitor();
        phoenixConfigMonitor.init(configuration.getKey(), this, envProperties);
    }

    /**
     * 同步方法
     *
     * @param dmls 数据包
     */
    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        try {
            phoenixSyncService.sync(mappingConfigCache, dmls, envProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ETL方法
     *
     * @param task   任务名, 对应配置名
     * @param params etl筛选条件
     * @return ETL结果
     */
    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = phoenixMapping.get(task);
        if (config != null) {
            DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            if (srcDataSource != null) {
                return PhoenixEtlService.importData(srcDataSource, dataSource, config, params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            // ds不为空说明传入的是destination
            for (MappingConfig configTmp : phoenixMapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(configTmp.getDataSourceKey());
                    if (srcDataSource == null) {
                        continue;
                    }
                    EtlResult etlRes = PhoenixEtlService.importData(srcDataSource, dataSource, configTmp, params);
                    if (!etlRes.getSucceeded()) {
                        resSucc = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSucc);
                if (resSucc) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }

    /**
     * 获取总数方法
     *
     * @param task 任务名, 对应配置名
     * @return 总数
     */
    @Override
    public Map<String, Object> count(String task) {
        Map<String, Object> res = new LinkedHashMap<>();
        MappingConfig config = phoenixMapping.get(task);
        if (config == null) {
            logger.info("[{}]phoenix config mapping: {}", this, phoenixMapping.keySet());
            res.put("succeeded", false);
            res.put("errorMessage", "Task[" + task + "] not found");
            res.put("tasks", phoenixMapping.keySet());
            return res;
        }
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        String sql = "SELECT COUNT(1) AS cnt FROM " + SyncUtil.getDbTableName(dbMapping);
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            Util.sqlRS(conn, sql, rs -> {
                try {
                    if (rs.next()) {
                        Long rowCount = rs.getLong("cnt");
                        res.put("count", rowCount);
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            });
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        res.put("targetTable", SyncUtil.getDbTableName(dbMapping));

        return res;
    }

    /**
     * 获取对应canal instance name 或 mq topic
     *
     * @param task 任务名, 对应配置名
     * @return destination
     */
    @Override
    public String getDestination(String task) {
        MappingConfig config = phoenixMapping.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }

    /**
     * 销毁方法
     */
    @Override
    public void destroy() {
        if (phoenixConfigMonitor != null) {
            phoenixConfigMonitor.destroy();
        }

        if (phoenixSyncService != null) {
            phoenixSyncService.close();
        }

        if (dataSource != null) {
            dataSource.close();
        }
    }
}
