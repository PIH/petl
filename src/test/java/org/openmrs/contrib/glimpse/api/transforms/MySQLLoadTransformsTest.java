package org.openmrs.contrib.glimpse.api.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.Serializable;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MySQLLoadTransformsTest implements Serializable {

    @Autowired
    private MySQLLoadTransforms mySQLLoadTransforms;

    @Test
    public void test()  {

        Pipeline pipeline = TestPipeline.create();

        PCollection output = pipeline.apply(mySQLLoadTransforms.getTransform());


        PAssert.that(output)
                .satisfies(new SerializableFunction<Iterable<Map<String, Object>>, Void>() {
                    @Override
                    public Void apply(Iterable<Map<String, Object>> input) {
                        for (Map<String, Object> element : input) {
                            // TODO actually do a test here?
                            System.out.println(element.get("patient_id"));
                            System.out.println(element.get("birthdate"));
                        }
                        return null;

                    }
                });

        pipeline.run();
    }
}
