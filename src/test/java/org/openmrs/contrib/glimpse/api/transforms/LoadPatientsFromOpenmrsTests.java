package org.openmrs.contrib.glimpse.api.transforms;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.openmrs.contrib.glimpse.api.transforms.LoadPatientsFromOpenMRS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class LoadPatientsFromOpenmrsTests {

    @Autowired
    LoadPatientsFromOpenMRS loadPatientsFromOpenMRS;

	@Test
	public void contextLoads() {

        loadPatientsFromOpenMRS.run();

	}

}
