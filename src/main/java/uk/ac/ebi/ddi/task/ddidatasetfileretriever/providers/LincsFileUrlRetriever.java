package uk.ac.ebi.ddi.task.ddidatasetfileretriever.providers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.util.UriComponentsBuilder;
import uk.ac.ebi.ddi.ddidomaindb.database.DB;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.DatasetFileUrlRetriever;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.IDatasetFileUrlRetriever;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class LincsFileUrlRetriever extends DatasetFileUrlRetriever {

    private static final String LINCS_ENDPOINT = "http://lincsportal.ccs.miami.edu/dcic/api";

    private static final Logger LOGGER = LoggerFactory.getLogger(LincsFileUrlRetriever.class);

    private ObjectMapper objectMapper = new ObjectMapper();

    public LincsFileUrlRetriever(IDatasetFileUrlRetriever datasetDownloadingRetriever) {
        super(datasetDownloadingRetriever);
    }

    @Override
    public Set<String> getAllDatasetFiles(String accession, String database) throws IOException {
        Set<String> result = new HashSet<>();
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(LINCS_ENDPOINT)
                .path("/fetchdata")
                .queryParam("limit", 1)
                .queryParam("searchTerm", "datasetid:" + accession)
                .queryParam("skip", 0);
        URI uri = builder.build().encode().toUri();
        ResponseEntity<String> responseEntity = execute(x -> restTemplate.getForEntity(uri, String.class));

        JsonNode res = objectMapper.readTree(responseEntity.getBody());
        if (res.get("results").get("totalDocuments").intValue() < 1) {
            return result;
        }
        JsonNode node = res.get("results").get("documents").elements().next();
        if (!node.has("levelspath")) {
            // Under Evaluation. i.e LDS-1237
            return result;
        }
        List<JsonNode> levelPaths = Lists.newArrayList(node.get("levelspath").elements());
        List<JsonNode> datasetLevels = Lists.newArrayList(node.get("datasetlevels").elements());

        for (int i = 0; i < levelPaths.size(); i++) {
            String path = levelPaths.get(i).asText().replace("/projects/ccs/bd2klincs/", "");
            UriComponentsBuilder fileUrl = UriComponentsBuilder.fromHttpUrl(LINCS_ENDPOINT)
                    .path("/download")
                    .queryParam("path", path)
                    .queryParam("file", datasetLevels.get(i).asText() + ".tar.gz");
            result.add(fileUrl.build().encode().toUriString());
        }
        return result;
    }

    @Override
    protected boolean isSupported(String database) {
        return database.equals(DB.LINCS.getDBName());
    }
}
