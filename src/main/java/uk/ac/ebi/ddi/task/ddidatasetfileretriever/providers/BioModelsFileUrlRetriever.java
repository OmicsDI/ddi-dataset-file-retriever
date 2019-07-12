package uk.ac.ebi.ddi.task.ddidatasetfileretriever.providers;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.util.UriComponentsBuilder;
import uk.ac.ebi.ddi.ddidomaindb.database.DB;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.DatasetFileUrlRetriever;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.IDatasetFileUrlRetriever;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class BioModelsFileUrlRetriever extends DatasetFileUrlRetriever {

    private static final String BIOMODEL_ENDPOINT = "https://www.ebi.ac.uk/biomodels/model";

    public BioModelsFileUrlRetriever(IDatasetFileUrlRetriever datasetDownloadingRetriever) {
        super(datasetDownloadingRetriever);
    }

    @Override
    public Set<String> getAllDatasetFiles(String accession, String database) throws IOException {
        Set<String> result = new HashSet<>();
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(BIOMODEL_ENDPOINT)
                .path("/files")
                .path("/" + accession);
        URI uriForFiles = builder.build().toUri();
        ResponseEntity<JsonNode> files = execute(x -> restTemplate.getForEntity(uriForFiles, JsonNode.class));
        for (JsonNode node : files.getBody().get("additional")) {
            UriComponentsBuilder urlFileBuilder = UriComponentsBuilder.fromHttpUrl(BIOMODEL_ENDPOINT)
                    .path("/download")
                    .path("/" + accession)
                    .queryParam("filename", node.get("name").asText());
            result.add(urlFileBuilder.build().encode().toUriString());
        }

        for (JsonNode node : files.getBody().get("main")) {
            UriComponentsBuilder urlFileBuilder = UriComponentsBuilder.fromHttpUrl(BIOMODEL_ENDPOINT)
                    .path("/download")
                    .path("/" + accession)
                    .queryParam("filename", node.get("name").asText());
            result.add(urlFileBuilder.build().encode().toUriString());
        }

        return result;
    }

    @Override
    protected boolean isSupported(String database) {
        return database.equals(DB.BIOMODELS.getDBName()) || database.equals(DB.BIOMODELS_SHORT.getDBName());
    }
}
