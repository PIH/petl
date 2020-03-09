package org.pih.petl.api.status;

import java.sql.Connection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.pih.petl.api.config.EtlDataSource;
import org.pih.petl.api.db.EtlConnectionManager;

/**
 * Methods for working with the ETL Status Table
 */
public class EtlStatusTable {

    public static final String ETL_STATUS_TBL = "etl_status";

    // TODO: Pull this from the configuration
    private static EtlDataSource getEtlStatusDataSource() {
        EtlDataSource ds = new EtlDataSource();
        ds.setDatabaseType("h2");
        ds.setDatabaseName("file:~/petl.db");
        ds.setUser("sa");
        ds.setPassword("Test123");
        ds.setOptions("DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE");
        return ds;
    }

    public static void createStatusTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("create table if not exists " + ETL_STATUS_TBL + " (");
        sb.append("  uuid            CHAR(36) NOT NULL, ");
        sb.append("  num             INT NOT NULL, ");
        sb.append("  table_name      VARCHAR(100) NOT NULL, ");
        sb.append("  total_expected  INT, ");
        sb.append("  total_loaded    INT, ");
        sb.append("  started         DATETIME NOT NULL, ");
        sb.append("  completed       DATETIME, ");
        sb.append("  status          VARCHAR(1000) NOT NULL,");
        sb.append("  error_message   VARCHAR(1000)");
        sb.append(")");
        executeUpdate(sb.toString());
    }

    public static String createStatus(String tableName) {
        String uuid = UUID.randomUUID().toString();
        executeUpdate("update " + ETL_STATUS_TBL + " set num = num+1 where table_name = ?", new Object[] {tableName});
        String stmt = "insert into " + ETL_STATUS_TBL + " (uuid, num, table_name, started, status) values (?,?,?,?,?)";
        executeUpdate(stmt, new Object[] { uuid, 1, tableName, new Date(), "Refresh initiated" });
        return uuid;
    }

    public static void updateTotalCount(String uuid, Integer totalCount) {
        String stmt = "update " + ETL_STATUS_TBL + " set total_expected = ? where uuid = ?";
        executeUpdate(stmt, new Object[] { totalCount, uuid });
    }

    public static void updateCurrentCount(String uuid, Integer num) {
        String stmt = "update " + ETL_STATUS_TBL + " set total_loaded = ? where uuid = ?";
        executeUpdate(stmt, new Object[] { num, uuid });
    }

    public static void updateStatus(String uuid, String message) {
        String stmt = "update " + ETL_STATUS_TBL + " set status = ? where uuid = ?";
        executeUpdate(stmt, new Object[] { message, uuid });
    }

    public static void updateStatusSuccess(String uuid) {
        String stmt = "update " + ETL_STATUS_TBL + " set status = ?, completed = ? where uuid = ?";
        executeUpdate(stmt, new Object[] { "Import Completed Sucessfully", new Date(), uuid });
    }

    public static void updateStatusError(String uuid, Exception e) {
        e.printStackTrace();
        String stmt = "update " + ETL_STATUS_TBL + " set status = ?, completed = ?, error_message = ? where uuid = ?";
        executeUpdate(stmt, new Object[] { "Import Failed", new Date(), e.getMessage(), uuid });
    }

    protected static void executeUpdate(String stmt, Object...params) {
        QueryRunner qr = new QueryRunner();
        try (Connection c = EtlConnectionManager.openConnection(getEtlStatusDataSource())) {
            qr.update(c, stmt, params);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Unable to execute status update", e);
        }
    }

    public static String getTableForUuid(String uuid) {
        QueryRunner qr = new QueryRunner();
        try (Connection c = EtlConnectionManager.openConnection(getEtlStatusDataSource())) {
            String sql = "select table_name from " + ETL_STATUS_TBL + " where uuid = ?";
            return qr.query(c, sql, new ScalarHandler<String>(), uuid);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Unable to get table for status of uuid " + uuid, e);
        }
    }

    public static Map<String, EtlStatus> getLatestEtlStatuses() {
        Map<String, EtlStatus> ret = new LinkedHashMap<String, EtlStatus>();
        StringBuilder sql = new StringBuilder();
        sql.append("select uuid, num, table_name, started, completed, total_expected, total_loaded, status, error_message ");
        sql.append("from " + ETL_STATUS_TBL + " ");
        sql.append("where table_name = ? and num = 1 ");
        QueryRunner qr = new QueryRunner();
        try (Connection c = EtlConnectionManager.openConnection(getEtlStatusDataSource())) {
            List<Map<String, Object>> l = qr.query(c, sql.toString(), new MapListHandler());
            if (l != null) {
                for (Map<String, Object> m : l) {
                    if (m != null) {
                        EtlStatus status = new EtlStatus();
                        status.setUuid((String) m.get("uuid"));
                        status.setNum((Integer) m.get("num"));
                        status.setTableName((String) m.get("table_name"));
                        status.setTotalExpected((Integer) m.get("total_expected"));
                        status.setTotalLoaded((Integer) m.get("total_loaded"));
                        status.setStarted((Date) m.get("started"));
                        status.setCompleted((Date) m.get("completed"));
                        status.setStatus((String) m.get("status"));
                        status.setErrorMessage((String) m.get("error_message"));
                        ret.put(status.getTableName(), status);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Unalbe to get latest ETL statuses", e);
        }
        return ret;
    }
}
