/**
 * The contents of this file are subject to the OpenMRS Public License
 * Version 1.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://license.openmrs.org
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) OpenMRS, LLC.  All Rights Reserved.
 */
package org.pih.petl.api.config;

import java.util.List;

public class StartupJobs {

	//***** PROPERTIES *****

	private List<String> jobs;
	private boolean exitAutomatically;

	//***** CONSTRUCTORS *****

	public StartupJobs() {}

    //***** ACCESSORS *****

    public List<String> getJobs() {
        return jobs;
    }

    public void setJobs(List<String> jobs) {
        this.jobs = jobs;
    }

    public boolean isExitAutomatically() {
        return exitAutomatically;
    }

    public void setExitAutomatically(boolean exitAutomatically) {
        this.exitAutomatically = exitAutomatically;
    }
}
