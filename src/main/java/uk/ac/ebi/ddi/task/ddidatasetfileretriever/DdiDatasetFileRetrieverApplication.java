package uk.ac.ebi.ddi.task.ddidatasetfileretriever;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import uk.ac.ebi.ddi.service.db.model.database.DatabaseDetail;
import uk.ac.ebi.ddi.service.db.model.dataset.Dataset;
import uk.ac.ebi.ddi.service.db.model.dataset.DatasetFile;
import uk.ac.ebi.ddi.service.db.service.database.DatabaseDetailService;
import uk.ac.ebi.ddi.service.db.service.dataset.DatasetFileService;
import uk.ac.ebi.ddi.service.db.service.dataset.IDatasetService;
import uk.ac.ebi.ddi.service.db.utils.DatasetUtils;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.configuration.DatasetFileRetrieveTaskProperties;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.exceptions.DatabaseNotFoundException;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.providers.*;

import java.util.*;
import java.util.stream.Collectors;

import static uk.ac.ebi.ddi.ddidomaindb.dataset.DSField.Configurations.IGNORE_DATASET_FILE_RETRIEVER;
import static uk.ac.ebi.ddi.service.db.utils.Constants.FROM_FILE_RETRIEVER;

@SpringBootApplication
public class DdiDatasetFileRetrieverApplication implements CommandLineRunner {

    private IDatasetFileUrlRetriever retriever = new DefaultDatasetFileUrlRetriever();

    private List<DatabaseDetail> databaseDetails;

    @Autowired
    private IDatasetService datasetService;

    @Autowired
    private DatabaseDetailService databaseDetailService;

    @Autowired
    private DatasetFileRetrieveTaskProperties taskProperties;

    @Autowired
    private DatasetFileService datasetFileService;

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
        databaseDetails = databaseDetailService.getDatabaseList();
        List<Dataset> datasets = datasetService.readDatasetHashCode(taskProperties.getDatabaseName());
        datasets.stream()
                .filter(x -> !DatasetUtils.isPrivateDataset(x))
                .forEach(x -> process(x, datasets.size()));
    }

    private DatabaseDetail getDatabase(String accession) {
        for (DatabaseDetail databaseDetail : databaseDetails) {
            if (databaseDetail.getAccessionPrefix() == null) {
                continue;
            }
            for (String prefix : databaseDetail.getAccessionPrefix()) {
                if (accession.startsWith(prefix)) {
                    return databaseDetail;
                }
            }
        }
        LOGGER.error("Database for dataset {} is not found", accession);
        throw new DatabaseNotFoundException();
    }

    private void process(Dataset ds, int total) {
        Dataset dataset = datasetService.read(ds.getAccession(), ds.getDatabase());
        Set<String> urls = new HashSet<>();
        try {
            if (!Objects.equals(DatasetUtils.getConfiguration(dataset, IGNORE_DATASET_FILE_RETRIEVER), "true")) {
                urls.addAll(retriever.getDatasetFiles(dataset.getAccession(), dataset.getDatabase()));
            }
            for (String secondaryAccession : dataset.getAllSecondaryAccessions()) {
                if (secondaryAccession.contains("~")) {
                    secondaryAccession = secondaryAccession.split("~")[0];
                }

                try {
                    String database = getDatabase(secondaryAccession).getDatabaseName();
                    if (database.equals(dataset.getDatabase())) {
                        // Subseries, ignoring
                        continue;
                    }
                    urls.addAll(retriever.getDatasetFiles(secondaryAccession, database));
                } catch (DatabaseNotFoundException ignore) {
                }
            }
            if (!urls.isEmpty()) {
                datasetFileService.deleteAll(ds.getAccession(), ds.getDatabase(), FROM_FILE_RETRIEVER);
                datasetFileService.saveAll(urls.stream()
                        .map(x -> new DatasetFile(ds.getAccession(), ds.getDatabase(), x, FROM_FILE_RETRIEVER))
                        .collect(Collectors.toList()));
            }
        } catch (DatabaseNotFoundException e) {
            LOGGER.info("Database for secondary accession of dataset {} not found", dataset.getAccession());
        } catch (Exception e) {
            String identity = dataset.getAccession() + " - " + dataset.getDatabase();
            LOGGER.error("Exception occurred with dataset {}, ", identity, e);
        } finally {
            calculatePercentFinished(dataset.getAccession(), total);
        }
    }

    private synchronized void calculatePercentFinished(String accession, int total) {
        processed.add(accession);
        if (processed.size() % 500 == 0) {
            LOGGER.info("Processed {}/{}", processed.size(), total);
        }
    }
}
