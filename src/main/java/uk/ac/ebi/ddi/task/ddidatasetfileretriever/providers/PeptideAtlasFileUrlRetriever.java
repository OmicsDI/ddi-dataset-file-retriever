package uk.ac.ebi.ddi.task.ddidatasetfileretriever.providers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

public class PeptideAtlasFileUrlRetriever extends DatasetFileUrlRetriever {

    private static final String FTP_PEPTIDEATLAS = "ftp://ftp.peptideatlas.org/pub/PeptideAtlas/Repository";

    private static final Logger LOGGER = LoggerFactory.getLogger(PeptideAtlasFileUrlRetriever.class);

    public PeptideAtlasFileUrlRetriever(IDatasetFileUrlRetriever datasetDownloadingRetriever) {
        super(datasetDownloadingRetriever);
    }

    @Override
    public Set<String> getAllDatasetFiles(String accession, String database) throws IOException {
        String url = String.format("%s/%s", FTP_PEPTIDEATLAS, accession);
        URI uri = UriUtils.toUri(url);
        try (DdiFPTClient ftpClient = createFtpClient(uri.getHost(), "anonymous", "anonymous")) {
            LOGGER.info("Connection status: {}", ftpClient.isConnected());
            return FtpUtils.getListFiles(ftpClient, uri.getPath(), "archive").stream()
                    .map(x -> String.format("ftp://%s%s", uri.getHost(), x))
                    .collect(Collectors.toSet());
        }
    }

    @Override
    protected boolean isSupported(String database) {
        return database.equals(DB.PEPTIDEATLAS.getDBName());
    }
}
