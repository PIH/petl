package org.openmrs.contrib.glimpse.api.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MySQLLoadTransformTest {

    private static NetworkServerControl derbyServer;
    private ClientDataSource dataSource;

    @Autowired
    private MySQLLoadTransform mySQLLoadTransform;

    @Before
    public void startDatabase() throws Exception {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        derbyServer = new NetworkServerControl(InetAddress.getByName("localhost"), 1527);
        derbyServer.start(null);

        dataSource = new ClientDataSource();
        dataSource.setCreateDatabase("create");
        dataSource.setDatabaseName("target/test");
        dataSource.setServerName("localhost");
        dataSource.setPortNumber(1527);

        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("create table patient(id INT, name VARCHAR(50))");
            }
        }

        mySQLLoadTransform.setAnalysisDataSource(dataSource);
    }

    @After
    public void shutDownDatabase() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("drop table patient");
            }
        } finally {
            if (derbyServer != null) {
                derbyServer.shutdown();
            }
        }
    }

    @Test
    public void testLoad() throws Exception {

        // create test data
        List<Map<String, String>> testData = new ArrayList<Map<String,String>>();

        Map<String, String> testEntry1 = new HashMap<String, String>();
        testEntry1.put("id", new String("2"));
        testEntry1.put("name", new String("2"));
        testData.add(testEntry1);

        Map<String, String> testEntry2 = new HashMap<String, String>();
        testEntry2.put("id", new String("4"));
        testEntry2.put("name", new String("4"));
        testData.add(testEntry2);

        // run the test
        Pipeline pipeline = TestPipeline.create();
        pipeline.apply(Create.of(testData).withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(mySQLLoadTransform.getTransform("patient", "id", Collections.singletonList("name")));

        pipeline.run();

        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from patient");
        resultSet.next();
        int count = resultSet.getInt(1);
        Assert.assertEquals(2, count);
    }

}
