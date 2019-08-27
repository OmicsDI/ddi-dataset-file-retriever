package uk.ac.ebi.ddi.task.ddidatasetfileretriever;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import uk.ac.ebi.ddi.service.db.model.dataset.Dataset;
import uk.ac.ebi.ddi.service.db.model.dataset.DatasetFile;
import uk.ac.ebi.ddi.service.db.service.dataset.DatasetFileService;
import uk.ac.ebi.ddi.service.db.service.dataset.IDatasetService;
import uk.ac.ebi.ddi.service.db.utils.DatasetCategory;
import uk.ac.ebi.ddi.service.db.utils.DatasetUtils;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.configuration.DatasetFileRetrieveTaskProperties;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.providers.*;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.services.SecondaryAccessionService;

import java.util.*;
import java.util.stream.Collectors;

import static uk.ac.ebi.ddi.ddidomaindb.dataset.DSField.Configurations.IGNORE_DATASET_FILE_RETRIEVER;
import static uk.ac.ebi.ddi.service.db.utils.Constants.FROM_FILE_RETRIEVER;

@SpringBootApplication
public class DdiDatasetFileRetrieverApplication implements CommandLineRunner {

    private IDatasetFileUrlRetriever retriever = new DefaultDatasetFileUrlRetriever();

    @Autowired
    private IDatasetService datasetService;

    @Autowired
    private DatasetFileRetrieveTaskProperties taskProperties;

    @Autowired
    private DatasetFileService datasetFileService;

    @Autowired
    private SecondaryAccessionService secondaryAccessionService;

    private static final Logger LOGGER = LoggerFactory.getLogger(DdiDatasetFileRetrieverApplication.class);

    private List<String> processed = new ArrayList<>();

    public DdiDatasetFileRetrieverApplication() {
        // Initializing retrievers
        retriever = new ArrayExpressFileUrlRetriever(retriever);
        retriever = new GEOFileUrlRetriever(retriever);
        retriever = new BioModelsFileUrlRetriever(retriever);
        retriever = new ExpressionAtlasFileUrlRetriever(retriever);
        retriever = new DbGapFileUrlRetriever(retriever);
        retriever = new GNPSFileUrlRetriever(retriever);
        retriever = new JPostFileUrlRetriever(retriever);
        retriever = new MassIVEFileUrlRetriever(retriever);
        retriever = new LincsFileUrlRetriever(retriever);
        retriever = new PeptideAtlasFileUrlRetriever(retriever);
        retriever = new EVAFileUrlRetriever(retriever);
        retriever = new MetabolightsFileUrlRetriever(retriever);
        retriever = new MetabolomicsWorkbenchFileUrlRetriever(retriever);
        retriever = new ENAFileUrlRetriever(retriever);
    }

    public static void main(String[] args) {
        SpringApplication.run(DdiDatasetFileRetrieverApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        List<Dataset> datasets = datasetService.readDatasetHashCode(taskProperties.getDatabaseName())
                .stream().filter(this::isDatasetNeedToFetchFiles)
                .collect(Collectors.toList());
        Collections.shuffle(datasets);
        datasets.forEach(x -> process(x, datasets.size()));
    }

    private boolean isDatasetNeedToFetchFiles(Dataset ds) {
        return ds.getCurrentStatus().equals(DatasetCategory.INSERTED.getType())
                || ds.getCurrentStatus().equals(DatasetCategory.UPDATED.getType())
                || ds.getCurrentStatus().equals(DatasetCategory.ANNOTATED.getType())
                || ds.getCurrentStatus().equals(DatasetCategory.ENRICHED.getType())
                || taskProperties.isForce();
    }

    private void process(Dataset ds, int total) {
        Dataset dataset = datasetService.read(ds.getAccession(), ds.getDatabase());
        try {
            if (Objects.equals(DatasetUtils.getConfiguration(dataset, IGNORE_DATASET_FILE_RETRIEVER), "true") ||
                    DatasetUtils.isPrivateDataset(dataset) || !isDatasetNeedToFetchFiles(ds)) {
                // We call the isDatasetNeedToFetchFiles one again for parallel computing
                return;
            }
            Map<String, Set<String>> urls = new HashMap<>();
            urls.put(ds.getAccession(), retriever.getDatasetFiles(dataset.getAccession(), dataset.getDatabase()));
            Map<String, List<String>> secondaryAccessions = secondaryAccessionService.getSecondaryAccessions(dataset);
            for (Map.Entry<String, List<String>> entry : secondaryAccessions.entrySet()) {
                for (String secondaryAccession : entry.getValue()) {
                    urls.put(secondaryAccession, retriever.getDatasetFiles(secondaryAccession, entry.getKey()));
                }
            }
            if (!urls.isEmpty()) {
                datasetFileService.deleteAll(ds.getAccession(), ds.getDatabase(), FROM_FILE_RETRIEVER);
                datasetFileService.saveAll(urls.keySet().stream()
                        .flatMap(accession -> urls.get(accession).stream()
                                .map(url -> new DatasetFile(ds.getAccession(), ds.getDatabase(), accession, url,
                                        FROM_FILE_RETRIEVER)))
                        .collect(Collectors.toList()));
            }
            dataset.setCurrentStatus(DatasetCategory.FILES_FETCHED.getType());
            datasetService.save(dataset);
        } catch (Exception e) {
            LOGGER.error("Exception occurred with dataset {}, ", ds, e);
        } finally {
            calculatePercentFinished(dataset.getAccession(), total);
        }
    }

    private synchronized void calculatePercentFinished(String accession, int total) {
        processed.add(accession);
        if (processed.size() % 100 == 0) {
            LOGGER.info("Processed {}/{}", processed.size(), total);
        }
    }
}
