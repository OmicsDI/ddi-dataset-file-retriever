package uk.ac.ebi.ddi.task.ddidatasetfileretriever.providers;

import uk.ac.ebi.ddi.ddidomaindb.database.DB;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.DatasetFileUrlRetriever;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.IDatasetFileUrlRetriever;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.utils.DdiFPTClient;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.utils.FtpUtils;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.utils.UriUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.stream.Collectors;


public class MetabolomicsWorkbenchFileUrlRetriever extends DatasetFileUrlRetriever {

    private static final String FTP_META_WB = "ftp://www.metabolomicsworkbench.org/Studies";

    public MetabolomicsWorkbenchFileUrlRetriever(IDatasetFileUrlRetriever datasetDownloadingRetriever) {
        super(datasetDownloadingRetriever);
    }

    @Override
    public Set<String> getAllDatasetFiles(String accession, String database) throws IOException {
        URI uri = UriUtils.toUri(FTP_META_WB);
        try (DdiFPTClient ftpClient = createFtpClient(uri.getHost(), "anonymous", "anonymous")) {
            return FtpUtils.getListFilesContaining(ftpClient, uri.getPath(), accession).stream()
                    .map(x -> String.format("ftp://%s%s", uri.getHost(), x))
                    .collect(Collectors.toSet());
        }
    }

    @Override
    protected boolean isSupported(String database) {
        return DB.METABOLOMICSWORKBENCH.getDBName().equals(database);
    }
}
