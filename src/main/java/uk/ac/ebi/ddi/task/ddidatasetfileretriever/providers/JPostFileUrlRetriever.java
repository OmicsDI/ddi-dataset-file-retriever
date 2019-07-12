package uk.ac.ebi.ddi.task.ddidatasetfileretriever.providers;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.util.UriComponentsBuilder;
import uk.ac.ebi.ddi.ddidomaindb.database.DB;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.DatasetFileUrlRetriever;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.IDatasetFileUrlRetriever;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class JPostFileUrlRetriever extends DatasetFileUrlRetriever {

    private static final String JPOST_ENDPOINT = "https://repository.jpostdb.org";

    private static final int DEFAULT_ELEMENTS_PER_REQUEST = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(JPostFileUrlRetriever.class);

    public JPostFileUrlRetriever(IDatasetFileUrlRetriever datasetDownloadingRetriever) {
        super(datasetDownloadingRetriever);
    }

    @Override
    public Set<String> getAllDatasetFiles(String accession, String database) throws IOException {
        Set<String> result = new HashSet<>();
        String jpostProject = getJpostProject(accession);
        if (jpostProject == null) {
            return result;
        }
        result.addAll(fetchListDatasetFiles(0, DEFAULT_ELEMENTS_PER_REQUEST, jpostProject));
        return result;
    }

    @Override
    protected boolean isSupported(String database) {
        return database.equals(DB.JPOST.getDBName());
    }


    private String getJpostProject(String accession) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(JPOST_ENDPOINT)
                .path("/_api/project")
                .queryParam("sortKey", "announcementDate")
                .queryParam("offset", 0)
                .queryParam("num", 1)
                .queryParam("target", "public")
                .queryParam("keyword", accession)
                .queryParam("searchKey", "");
        URI uri = builder.build().encode().toUri();
        ResponseEntity<JsonNode> response = execute(x -> restTemplate.getForEntity(uri, JsonNode.class));
        for (JsonNode node : response.getBody()) {
            return node.get("jpostId").asText();
        }
        return null;
    }

    private List<String> fetchListDatasetFiles(int offset, int numberElement, String accession) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(JPOST_ENDPOINT)
                .path("/_api/file")
                .queryParam("sortKey", "id")
                .queryParam("offset", offset)
                .queryParam("num", numberElement)
                .queryParam("target", "server")
                .queryParam("projectId", accession)
                .queryParam("searchKey", "");
        URI uri = builder.build().encode().toUri();
        List<String> result = new ArrayList<>();
        ResponseEntity<JsonNode> response = execute(x -> restTemplate.getForEntity(uri, JsonNode.class));
        if (response.getStatusCode() != HttpStatus.OK) {
            LOGGER.error("Exception occurred when fetching dataset's files of {}", accession);
            return result;
        }

        for (JsonNode item : response.getBody().get("list")) {
            String fileName = item.get("fileName").asText();
            String projectId = item.get("projectId").asText();
            UriComponentsBuilder fileUrlBuilder = UriComponentsBuilder.fromHttpUrl(JPOST_ENDPOINT)
                    .path("/data")
                    .path("/" + projectId)
                    .path("/" + fileName);
            result.add(fileUrlBuilder.build().encode().toUriString());
        }

        int totalFiles = response.getBody().get("total").intValue();
        int fetched = offset + numberElement;
        if (offset + numberElement < totalFiles) {
            result.addAll(fetchListDatasetFiles(fetched, totalFiles - fetched, accession));
        }
        return result;
    }
}
