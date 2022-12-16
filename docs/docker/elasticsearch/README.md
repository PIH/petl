# Elastic Search

## Overview

This project simply serves to get an Elastic Search instance up for use in testing, along with Kibana.  The actual database creation and usage can be found in other examples that use this Elastic Search instance as a repository.

### Executing SQL Queries against Elastic Search

This can be done in Kibana using the Dev Tools Console:

Connect to:  http://localhost:5601/app/dev_tools#/console

Execute a command like the following:
```shell
POST _sql?format=txt
{
"query": "select * from person_index"
}
```