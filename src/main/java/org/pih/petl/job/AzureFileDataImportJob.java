package org.pih.petl.job;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;
import org.pih.petl.api.JobExecution;
import org.pih.petl.job.config.DataSource;
import org.pih.petl.job.config.JobConfigReader;
import org.pih.petl.job.config.TableColumn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * PetlJob that can load metadata from Azure files to a table
 * See:  <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs?tabs=microsoft-entra-id">...</a>
 */
@Component("azure-file-data-import")
public class AzureFileDataImportJob implements PetlJob {

    private final Log log = LogFactory.getLog(getClass());

    private static final String[] DATE_FORMATS = {
            "EE, dd MMM yyyy HH:mm:ss z",
            "EE, dd MMM yyyy HH:mm:ss",
            "dd MMM yyyy"
    };

    @Autowired
    ApplicationConfig applicationConfig;

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final JobExecution jobExecution) throws Exception {

        log.debug("Executing " + getClass().getSimpleName());
        JobConfigReader configReader = new JobConfigReader(applicationConfig, jobExecution.getJobConfig());

        // Get source datasource
        DataSource sourceDataSource = configReader.getDataSource("source", "datasource");
        if (sourceDataSource == null) {
            throw new PetlException("Source Datasource is required");
        }
        String url = sourceDataSource.getUrl();
        if (StringUtils.isBlank(url)) {
            throw new PetlException("Source Datasource url is required");
        }

        // Get target datasource
        DataSource targetDataSource = configReader.getDataSource("target", "datasource");
        if (targetDataSource == null) {
            throw new PetlException("Target Datasource is required");
        }
        String targetTable = configReader.getString("target", "table");
        if (StringUtils.isBlank(targetTable)) {
            throw new PetlException("Target Table is required");
        }
        String targetSchema = configReader.getFileContents("target", "schema");
        if (StringUtils.isBlank(targetSchema)) {
            throw new PetlException("Target Schema is required");
        }

        // Test source connection
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url + "&maxresults=1");
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                prettyPrintXml(response.getEntity().getContent());
                if (response.getStatusLine().getStatusCode() == 200) {
                    log.debug("Connection to source successful");
                }
                else {
                    throw new PetlException("Connection to source failed: " + response.getStatusLine());
                }
            }
        }

        // Test target connection
        if (targetDataSource.testConnection()) {
            log.debug("Connection to target successful");
        }
        else {
            throw new PetlException("Connection to target failed");
        }

        // If this is not configured to only test, then execute the remaining process
        boolean testOnly = configReader.getBoolean(false, "testOnly");
        if (!testOnly) {
            int blobsImported = 0;
            int batchesPerformed = 0;
            String nextBatchMarker = null;
            int batchSize = configReader.getInt(1000, "source", "batchSize");
            while (batchesPerformed == 0 || nextBatchMarker != null) {
                log.info("Executing Batch #" + (batchesPerformed+1));
                try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                    String batchUrl = url + "&maxresults=" + batchSize;
                    if (nextBatchMarker != null) {
                        batchUrl += "&marker=" + nextBatchMarker;
                    }
                    HttpGet httpGet = new HttpGet(batchUrl);
                    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                        log.debug("Retrieving data from Azure using marker: " + nextBatchMarker);
                        Batch batch = parseXml(response.getEntity().getContent());
                        log.debug("Retrieved " + batch.getBlobs().size() + " blobs from Azure");
                        log.debug("Retrieved a marker for a next iteration: " + batch.getMarker());

                        // If this is the first batch, then recreate the target schema after first successful retrieval
                        if (batchesPerformed == 0) {
                            log.info("This is the first batch, recreating target schema");
                            targetDataSource.dropTableIfExists(targetTable);
                            targetDataSource.executeUpdate(targetSchema);
                        }

                        List<TableColumn> targetColumns = targetDataSource.getTableColumns(targetTable);

                        StringBuilder insertStmt = new StringBuilder();
                        insertStmt.append("insert into ").append(targetTable).append("(");
                        for (int i=0; i<targetColumns.size(); i++) {
                            insertStmt.append(i == 0 ? "" : ", ");
                            insertStmt.append(targetColumns.get(i).getName());
                        }
                        insertStmt.append(") values (");
                        for (int i=1; i<=targetColumns.size(); i++) {
                            insertStmt.append("?");
                            insertStmt.append(i == targetColumns.size() ? ")" : ", ");
                        }
                        insertStmt.append(";");
                        log.trace("Created insert statement");
                        log.trace(insertStmt.toString());

                        // Process batch
                        try (Connection conn = targetDataSource.openConnection()) {
                            try (PreparedStatement ps = conn.prepareStatement(insertStmt.toString())) {
                                for (Map<String, String> row : batch.getBlobs()) {
                                    for (int i=0; i<targetColumns.size(); i++) {
                                        int columnIndex = i+1;
                                        TableColumn column = targetColumns.get(i);
                                        String columnType = column.getType().toLowerCase();
                                        boolean isDate = columnType.contains("date") || columnType.contains("time");
                                        String value = row.get(column.getName());
                                        if (isDate) {
                                            ps.setObject(columnIndex, parseIntoDate(value));
                                        }
                                        else {
                                            ps.setObject(columnIndex, value);
                                        }
                                    }
                                    ps.addBatch();
                                    ps.clearParameters();
                                }
                                log.debug("Executing batch insert");
                                int[] updateCounts = ps.executeBatch();
                                log.info("Batch insert completed. Num rows affected: " + updateCounts.length);
                                blobsImported += updateCounts.length;
                            }
                        }

                        nextBatchMarker = batch.getMarker();
                        batchesPerformed++;
                    }
                }
            }
            log.info("Import completed.  Imported " + blobsImported + " in " + batchesPerformed + " batches");
        }
    }

    /**
     * Parses the input stream xml for the given query and returns this as a Batch of data
     */
    protected Batch parseXml(InputStream content) throws Exception {
        Batch batch = new Batch();
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        XMLEventReader reader = xmlInputFactory.createXMLEventReader(content);
        Map<String, String> blob = null;
        while (reader.hasNext()) {
            XMLEvent event = reader.nextEvent();
            if (event.isStartElement()) {
                String elementName = normalizeElementName(event.asStartElement().getName().getLocalPart());
                if (elementName.equals("blob")) {
                    blob = new LinkedHashMap<>();
                }
                else if (elementName.equals("nextmarker")) {
                    event = reader.nextEvent();
                    if (event.isCharacters()) {
                        batch.setMarker(event.asCharacters().getData());
                    }
                }
                else if (blob != null) {
                    event = reader.nextEvent();
                    if (event.isCharacters()) {
                        blob.put(elementName, event.asCharacters().getData());
                    }
                }
            }
            if (event.isEndElement()) {
                String elementName = normalizeElementName(event.asEndElement().getName().getLocalPart());
                if (elementName.equals("blob")) {
                    batch.addBlob(blob);
                    blob = null;
                }
            }
        }
        return batch;
    }

    protected String normalizeElementName(String elementName) {
        return elementName.toLowerCase().replace("-", "_");
    }

    protected Date parseIntoDate(String value) {
        if (StringUtils.isNotBlank(value)) {
            try {
                return DateUtils.parseDate(value, DATE_FORMATS);
            }
            catch (Exception e) {
                log.warn("Unable to parse " + value + " into a date, skipping");
            }
        }
        return null;
    }

    /**
     * Outputs the XML returned from the entity with TRACE level logging as pretty-printed XML for debugging
     */
    protected void prettyPrintXml(InputStream inputStream) throws Exception {
        InputSource src = new InputSource(inputStream);
        Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(src);
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        Writer out = new StringWriter();
        transformer.transform(new DOMSource(document), new StreamResult(out));
        log.trace(System.lineSeparator() + out);
    }

    public static class Batch {

        private final List<Map<String, String>> blobs = new ArrayList<>();
        private String marker = null;

        public Batch() {}

        public List<Map<String, String>> getBlobs() {
            return blobs;
        }

        public void addBlob(Map<String, String> blob) {
            blobs.add(blob);
        }

        public String getMarker() {
            return marker;
        }

        public void setMarker(String marker) {
            this.marker = marker;
        }
    }
}
