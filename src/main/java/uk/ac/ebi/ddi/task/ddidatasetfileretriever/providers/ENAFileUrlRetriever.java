package uk.ac.ebi.ddi.task.ddidatasetfileretriever.providers;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import uk.ac.ebi.ddi.ddidomaindb.database.DB;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.DatasetFileUrlRetriever;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.IDatasetFileUrlRetriever;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ENAFileUrlRetriever extends DatasetFileUrlRetriever {

    private static final String ENA_ENDPOINT = "www.ebi.ac.uk/ena/portal/api";

    private static final String ENAREADRUNCOLUMN = "study_accession,fastq_ftp,fastq_aspera,fastq_galaxy";
    private static final String ENAWSGFILECOLUMN = "study_accession,embl_file,fasta_file,master_file";

    private static final String ENAANALYSISCOLUMN = "study_accession,submitted_ftp,submitted_aspera,submitted_galaxy";

    private Pattern assemblyPattern = Pattern.compile("<URL>([\\s\\S]*?)<\\/URL>");

    private static final Logger LOGGER = LoggerFactory.getLogger(ENAFileUrlRetriever.class);

    private static final int N_ASSEMBLY_FILES_PER_REQUEST = 200;

    public ENAFileUrlRetriever(IDatasetFileUrlRetriever datasetDownloadingRetriever) {
        super(datasetDownloadingRetriever);
    }

    @Override
    public Set<String> getAllDatasetFiles(String accession, String database) {
        Set<String> result = new HashSet<>();
        try {
            result.addAll(getReadRunFiles(accession));
            result.addAll(getAnalysisFiles(accession));
            result.addAll(getAssemblyFiles(accession));
            result.addAll(getWgsFiles(accession));
        } catch (URISyntaxException ex) {
            LOGGER.error("uri syntax exception in get all dataset " +
                    "files method of ena file retriever with acc {}, ", accession, ex);
        }
        return result;
    }

    private Set<String> getReadRunFiles(String accession) throws URISyntaxException {
        Set<String> result = new HashSet<>();
        URI uri = new URIBuilder()
                .setScheme("https")
                .setHost(ENA_ENDPOINT)
                .setPath("/search")
                .setParameter("query", "(study_accession=" + accession + ")")
                .setParameter("fields", ENAREADRUNCOLUMN)
                .setParameter("result", "read_run")
                .setParameter("limit", "0")
                .setParameter("format", "json")
                .build();
        ResponseEntity<JsonNode> files = execute(x -> restTemplate.getForEntity(uri, JsonNode.class));

        if (files.getBody() != null) {
            for (JsonNode node : files.getBody()) {
                result.addAll(Arrays.asList(node.get("fastq_ftp").asText().split(";")));
            }
        }

        return result;
    }

    private Set<String> getAnalysisFiles(String accession) throws URISyntaxException {
        Set<String> result = new HashSet<>();

        URI uri = new URIBuilder()
                .setScheme("https")
                .setHost(ENA_ENDPOINT)
                .setPath("/search")
                .setParameter("query", "(study_accession=" + accession + ")")
                .setParameter("fields", ENAANALYSISCOLUMN)
                .setParameter("result", "analysis")
                .setParameter("limit", "0")
                .setParameter("format", "json")
                .build();

        ResponseEntity<JsonNode> files = execute(x -> restTemplate.getForEntity(uri, JsonNode.class));

        if (files.getBody() != null) {
            for (JsonNode node : files.getBody()) {
                result.addAll(Arrays.asList(node.get("submitted_ftp").asText().split(";")));
            }
        }

        return result;
    }

    private Set<String> getAssemblyFiles(String accession) throws URISyntaxException {
        Set<String> result = new HashSet<>();
        URI uri = new URIBuilder()
                .setScheme("https")
                .setHost(ENA_ENDPOINT)
                .setPath("/search")
                .setParameter("query", "(study_accession=" + accession + ")")
                .setParameter("fields", "accession")
                .setParameter("result", "assembly")
                .setParameter("limit", "0")
                .setParameter("format", "json")
                .build();
        ResponseEntity<JsonNode> files = execute(x -> restTemplate.getForEntity(uri, JsonNode.class));

        List<String> accessions = new ArrayList<>();

        if (files.getBody() != null) {
            for (JsonNode node : files.getBody()) {
                accessions.add(node.get("accession").textValue() + ".1");
            }
        }

        List<List<String>> partitions = Lists.partition(accessions, N_ASSEMBLY_FILES_PER_REQUEST);

        for (List<String> chunk : partitions) {
            URI viewUri = new URIBuilder()
                    .setScheme("https")
                    .setHost("www.ebi.ac.uk/ena/data/view")
                    .setPath("/" + String.join(",", chunk) + "&display=xml")
                    .build();
            ResponseEntity<String> assemblyFiles = execute(x -> restTemplate.getForEntity(viewUri, String.class));
            if (assemblyFiles.getBody() != null) {
                Matcher matcher = assemblyPattern.matcher(assemblyFiles.getBody());
                while(matcher.find()) {
                    result.add(matcher.group(1));
                }
            }
        }

        return result;
    }

    private Set<String> getWgsFiles(String accession) throws URISyntaxException {
        Set<String> result = new HashSet<>();
        URI uri = new URIBuilder()
                .setScheme("https")
                .setHost(ENA_ENDPOINT)
                .setPath("/search")
                .setParameter("query", "(study_accession=" + accession + ")")
                .setParameter("fields", ENAWSGFILECOLUMN)
                .setParameter("result", "wgs_set")
                .setParameter("limit", "0")
                .setParameter("format", "json")
                .build();
        ResponseEntity<JsonNode> files = execute(x -> restTemplate.getForEntity(uri, JsonNode.class));

        if (files.getBody() != null) {
            for (JsonNode node : files.getBody()) {
                result.add(node.get("embl_file").textValue());
                result.add(node.get("fasta_file").textValue());
                result.add(node.get("master_file").textValue());
            }
        }
        return result;
    }
    @Override
    protected boolean isSupported(String database) {
        return DB.ENA.getDBName().equals(database);
    }

}
