package uk.ac.ebi.ddi.task.ddidatasetfileretriever.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.ac.ebi.ddi.service.db.model.database.DatabaseDetail;
import uk.ac.ebi.ddi.service.db.model.dataset.Dataset;
import uk.ac.ebi.ddi.service.db.service.database.DatabaseDetailService;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.exceptions.DatabaseNotFoundException;

import java.util.*;

@Service
public class SecondaryAccessionService {

    private List<DatabaseDetail> databaseDetails;

    private static final Logger LOGGER = LoggerFactory.getLogger(SecondaryAccessionService.class);

    @Autowired
    public SecondaryAccessionService(DatabaseDetailService databaseDetailService) {
        databaseDetails = databaseDetailService.getDatabaseList();
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

    public Map<String, List<String>> getSecondaryAccessions(Dataset dataset) {
        Map<String, List<String>> result = new HashMap<>();
        for (String acc : dataset.getAllSecondaryAccessions()) {

            String secondaryAccession = acc.contains("~") ? acc.split("~")[0] : acc;

            try {
                String database = getDatabase(secondaryAccession).getDatabaseName();
                if (database.equals(dataset.getDatabase())) {
                    // Subseries, ignoring
                    continue;
                }

                if (result.containsKey(database)) {
                    result.get(database).add(secondaryAccession);
                } else {
                    result.put(database, new ArrayList<>(Collections.singleton(secondaryAccession)));
                }
            } catch (DatabaseNotFoundException ignore) {
                LOGGER.info("Database for dataset {} isn't found", dataset.getAccession());
            }
        }
        return result;
    }
}
