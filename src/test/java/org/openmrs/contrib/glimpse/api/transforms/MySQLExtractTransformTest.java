package org.openmrs.contrib.glimpse.api.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.Serializable;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MySQLExtractTransformTest implements Serializable {

    private static NetworkServerControl derbyServer;
    private ClientDataSource dataSource;

    @Autowired
    private MySQLExtractTransform mySQLExtractTransforms;

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

        mySQLExtractTransforms.setOpenmrsDataSource(dataSource);
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

    // taken from beam tests
    public void initTable() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("delete from patient");
            }

            String[] patients = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};
            connection.setAutoCommit(false);
            try (PreparedStatement preparedStatement =
                         connection.prepareStatement("insert into patient " + "values (?,?)")) {
                for (int i = 0; i < patients.length; i++) {
                    preparedStatement.clearParameters();
                    preparedStatement.setInt(1, i + 1);
                    preparedStatement.setString(2, patients[i]);
                    preparedStatement.executeUpdate();
                }
            }

            connection.commit();
        }
    }

    @Test
    public void testExtraction() throws Exception {

        // add test data
        initTable();

        Pipeline pipeline = TestPipeline.create();

        PCollection output = pipeline.apply(mySQLExtractTransforms.getTransform("sql/extract-test.sql"));

        PAssert.that(output)
                .satisfies(new SerializableFunction<Iterable<Map<String, Object>>, Void>() {
                    @Override
                    public Void apply(Iterable<Map<String, Object>> input) {
                        int i = 0;
                        for (Map<String, Object> element : input) {
                            Integer id = (Integer) element.get("ID");
                            String name = (String) element.get("NAME");

                            assertThat(id, is(Integer.valueOf(name)));

                          /*  for (Map.Entry<String, Object> entry : element.entrySet()) {
                                System.out.println(entry.getKey() + ": " + entry.getValue());
                            }*/

                            i++;

                        }
                        assertThat(i, is(10));
                        return null;
                    }
                });

        pipeline.run();
    }
}
