FROM mcr.microsoft.com/mssql/server:2019-CU13-ubuntu-20.04

ENV ACCEPT_EULA=Y
ENV SA_PASSWORD="9%4qP7b2H!%J"
ENV DATABASE_NAME="openmrs_reporting"

COPY entrypoint.sh /
COPY Dockerfile /

ENTRYPOINT [ "/bin/bash", "entrypoint.sh" ]
CMD [ "/opt/mssql/bin/sqlservr" ]