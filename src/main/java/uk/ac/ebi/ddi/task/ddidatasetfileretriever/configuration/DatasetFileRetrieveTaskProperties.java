package uk.ac.ebi.ddi.task.ddidatasetfileretriever.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("file-retriever")
public class DatasetFileRetrieveTaskProperties {

    private String databaseName;

    private boolean force = false;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    @Override
    public String toString() {
        return "DatasetFileRetrieveTaskProperties{" +
                "databaseName='" + databaseName + '\'' +
                ", force=" + force +
                '}';
    }
}
