package org.pih.petl.job.config;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Encapsulates a basic schedule configuration for jobs
 */
public class TableColumn {

    private String name;
    private String type;
    private String value;

    public TableColumn() {}

    public TableColumn(String name, String type, String value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        return name + " " + type + (value != null ? " = " + value : "");
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(name == null ? null : name.toLowerCase())
                .append(type == null ? null : type.toLowerCase())
                .append(value == null ? null : value.toLowerCase())
                .toHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TableColumn)) { return false; }
        TableColumn that = (TableColumn) o;
        return new EqualsBuilder()
                .append(name == null ? null : name.toLowerCase(), that.getName() == null ? null : that.getName().toLowerCase())
                .append(type == null ? null : type.toLowerCase(), that.getType() == null ? null : that.getType().toLowerCase())
                .append(value == null ? null : type.toLowerCase(), that.getValue() == null ? null : that.getValue().toLowerCase())
                .isEquals();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
