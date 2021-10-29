/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.pih.petl;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.config.TableColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods useful for manipulating SQL statements
 */
public class SqlUtils {

	private static Log log = LogFactory.getLog(SqlUtils.class);

	// Regular expression to identify a change in the delimiter.  This ignores spaces, allows delimiter in comment, allows an equals-sign
    private static final Pattern DELIMITER_PATTERN = Pattern.compile("^\\s*(--)?\\s*delimiter\\s*=?\\s*([^\\s]+)+\\s*.*$", Pattern.CASE_INSENSITIVE);

    /**
     * @return a List of statements that are parsed out of the passed sql, ignoring comments, and respecting delimiter assignment
     */
    public static List<String> parseSqlIntoStatements(String sql, String currentDelimiter) {
	    List<String> statements = new ArrayList<String>();
	    StringBuilder currentStatement = new StringBuilder();

	    boolean inMultiLineComment = false;

	    for (String line : sql.split("\\r?\\n")) {

	        // First, trim the line and remove any trailing comments in the form of "statement;  -- Comments here"
	        int delimiterIndex = line.indexOf(currentDelimiter);
	        int dashCommentIndex = line.indexOf("--");
	        if (delimiterIndex > 0 && delimiterIndex < dashCommentIndex) {
                line = line.substring(0, dashCommentIndex);
            }
            line = line.trim();

	        // Check to see if this line is within a multi-line comment, or if it ends a multi-line comment
	        if (inMultiLineComment) {
	            if (isEndOfMultiLineComment(line)) {
	                inMultiLineComment = false;
                }
            }
            // If we are not within a multi-line comment, then process the line, if it is not a single line comment or empty space
            else {
                if (!isEmptyLine(line) && !isSingleLineComment(line)) {

                    // If this line starts a multi-line comment, then ignore it and mark for next iteration
                    if (isStartOfMultiLineComment(line)) {
                        inMultiLineComment = true;
                    }
                    else {
                        // If the line is serving to set a new delimiter, set it and continue
                        Matcher matcher = DELIMITER_PATTERN.matcher(line);
                        if (matcher.matches()) {
                            currentDelimiter = matcher.group(2);
                        }
                        else {
                            // If we are here, that means that this line is part of an actual sql statement
                            if (line.endsWith(currentDelimiter)) {
                                line = line.substring(0, line.lastIndexOf(currentDelimiter));
                                currentStatement.append(line);
                                statements.add(currentStatement.toString());
                                currentStatement = new StringBuilder();
                            }
                            else {
                                currentStatement.append(line).append("\n");
                            }
                        }
                    }
                }
            }
        }
        if (currentStatement.length() > 0) {
            statements.add(currentStatement.toString());
        }
        return statements;
    }

    /**
     * Manipulates a base create table statement and adds additional columns
     */
    public static String addExtraColumnsToSchema(String schema, List<TableColumn> extraColumns) {
        if (schema != null) {
            if (extraColumns != null && !extraColumns.isEmpty()) {
                schema = schema.trim();
                schema = schema.substring(0, schema.lastIndexOf(")"));
                for (TableColumn extraColumn : extraColumns) {
                    schema += ", " + extraColumn.getName() + " " + extraColumn.getType();
                }
                schema += ")";
            }
        }
        return schema;
    }

    /**
     * Manipulates a base create table statement and adds in partition scheme
     */
    public static String addPartitionSchemeToSchema(String schema, String partitionScheme, String partitionColumn) {
        if (schema != null) {
            if (StringUtils.isNotEmpty(partitionScheme) && StringUtils.isNotEmpty(partitionColumn)) {
                schema = schema.trim();
                if (schema.endsWith(";")) {
                    schema = schema.substring(0, schema.length()-1);
                }
                schema += " ON " + partitionScheme + "(" + partitionColumn + ")";
            }
        }
        return schema;
    }

    /**
     * Manipulates a base select statement and adds additional static column values to select at the end
     */
    public static String addExtraColumnsToSelect(String query, List<TableColumn> extraColumns) {
        StringBuilder extraColumnClause = new StringBuilder();
        if (extraColumns != null) {
            for (TableColumn c : extraColumns) {
                extraColumnClause.append(", ").append(c.getValue()).append(" as ").append(c.getName());
            }
        }
        if (extraColumnClause.length() == 0) {
            return query;
        }
        Pattern whitespace = Pattern.compile("\\sfrom\\s");
        Matcher matcher = whitespace.matcher(query.toLowerCase());
        if (matcher.find()) {
            int startIndex = matcher.start();
            return query.substring(0, startIndex) + extraColumnClause + query.substring(startIndex);
        }
        else {
            throw new IllegalArgumentException("Unable to find the ' from ' key word in query: " + query);
        }
    }

    /**
     * Returns the name of the table that the passed schema SQL is creating
     */
    public static String getTableName(String schemaSql) {
        StringBuilder ret = new StringBuilder();
        for (String word : schemaSql.split("\\s")) {
            if (ret.toString().trim().toLowerCase().endsWith("create table")) {
                return word;
            }
        }
        throw new PetlException("No table name found in the given schema sql: " + schemaSql);
    }

    /**
     * This method takes a schema create table statement, and adds the given suffix to the table name it is creating
     */
    public static String addSuffixToCreatedTablename(String schemaSql, String suffix) {
        StringBuilder ret = new StringBuilder();
        for (String word : schemaSql.split("\\s")) {
            if (ret.toString().trim().toLowerCase().endsWith("create table")) {
                word = word + suffix;
            }
            ret.append(word).append(" ");
        }
        return ret.toString();
    }

    public static String createMovePartitionStatement(String sourceTable, String destinationTable, String partitionNum) {
        StringBuilder sb = new StringBuilder();
        sb.append("TRUNCATE TABLE ").append(destinationTable).append(" WITH (PARTITIONS (").append(partitionNum).append("));");
        sb.append(System.lineSeparator());
        sb.append("ALTER TABLE ").append(sourceTable).append(" SWITCH PARTITION ").append(partitionNum).append(" TO ").append(destinationTable).append(" PARTITION ").append(partitionNum).append(";");
        return sb.toString();
    }

    //********** CONVENIENCE METHODS **************

    protected static boolean isEmptyLine(String line) {
        return line == null || StringUtils.isBlank(line);
    }

    protected static boolean isSingleLineComment(String line) {
        return line.startsWith("--") || line.startsWith("//") || (isStartOfMultiLineComment(line) && isEndOfMultiLineComment(line));
    }

    protected static boolean isStartOfMultiLineComment(String line) {
        return line.startsWith("/*");
    }

    protected static boolean isEndOfMultiLineComment(String line) {
        return line.endsWith("*/");
    }
}
