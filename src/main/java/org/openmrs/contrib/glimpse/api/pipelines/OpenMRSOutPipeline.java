package org.openmrs.contrib.glimpse.api.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.openmrs.contrib.glimpse.api.transforms.MySQLExtractTransform;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
public class OpenMRSOutPipeline implements Serializable {


    @Autowired
    private MySQLExtractTransform mySQLExtractTransforms;

    /**
     * This simply creates a new Pipeline and wires it together, linking the output from the read transform to the input of the write transform
     */
    public void run() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        PCollection output = p.apply(mySQLExtractTransforms.getTransform("sql/extract-patients.sql"));
        p.run();
    }

}
