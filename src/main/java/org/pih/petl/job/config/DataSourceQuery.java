package org.pih.petl.job.config;

/**
 * Configuration for a query to a data source
 */
public class DataSourceQuery {

    private String datasource;
    private String query;
    private String queryPath;

    public DataSourceQuery() {}

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getQueryPath() {
        return queryPath;
    }

    public void setQueryPath(String queryPath) {
        this.queryPath = queryPath;
    }
}
