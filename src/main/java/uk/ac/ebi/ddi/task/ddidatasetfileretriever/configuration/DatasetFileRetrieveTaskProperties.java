package uk.ac.ebi.ddi.task.ddidatasetfileretriever.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("file-retriever")
public class DatasetFileRetrieveTaskProperties {

    private String databaseName;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}
