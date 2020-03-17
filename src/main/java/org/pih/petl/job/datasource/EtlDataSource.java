package org.pih.petl.job.datasource;

import org.apache.commons.lang.StringUtils;
import org.pih.petl.PetlException;

/**
 * Encapsulates a data source configuration
 */
public class EtlDataSource {

    private String databaseType;
    private String host;
    private String port;
    private String databaseName;
    private String options;
    private String url; // Alternative to the above piecemeal settings
    private String user;
    private String password;

    //***** CONSTRUCTORS *****

    public EtlDataSource() {}

    //***** INSTANCE METHODS *****

    public String getJdbcUrl() {
        if (StringUtils.isNotBlank(url)) {
            return url;
        }
        else {
            StringBuilder sb = new StringBuilder();
            if ("mysql".equalsIgnoreCase(databaseType)) {
                sb.append("jdbc:mysql://").append(host).append(":").append(port);
                sb.append("/").append(databaseName).append("?");
                if (StringUtils.isNotBlank(options)) {
                    sb.append(options);
                }
                else {
                    sb.append("autoReconnect=true");
                    sb.append("&sessionVariables=default_storage_engine%3DInnoDB");
                    sb.append("&useUnicode=true");
                    sb.append("&characterEncoding=UTF-8");
                }
            }
            else if ("sqlserver".equalsIgnoreCase(databaseType)) {
                sb.append("jdbc:sqlserver://").append(host).append(":").append(port);
                sb.append(";").append("database=").append(databaseName);
                if (StringUtils.isNotBlank(options)) {
                    sb.append(";").append(options);
                }
            }
            else if ("h2".equalsIgnoreCase(databaseType)) {
                sb.append("jdbc:h2:").append(databaseName);
                if (StringUtils.isNotBlank(options)) {
                    sb.append(";").append(options);
                }
            }
            else {
                throw new PetlException("Currently only mysql, sqlserver, or h2 database types are supported");
            }
            return sb.toString();
        }
    }

    //***** PROPERTY ACCESS *****

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getOptions() {
        return options;
    }

    public void setOptions(String options) {
        this.options = options;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
