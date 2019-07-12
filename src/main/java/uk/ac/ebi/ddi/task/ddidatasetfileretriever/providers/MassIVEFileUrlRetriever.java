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


public class MassIVEFileUrlRetriever extends DatasetFileUrlRetriever {

    private static final String FTP_MASSIVE = "ftp://massive.ucsd.edu";

    public MassIVEFileUrlRetriever(IDatasetFileUrlRetriever datasetDownloadingRetriever) {
        super(datasetDownloadingRetriever);
    }

    @Override
    public Set<String> getAllDatasetFiles(String accession, String database) throws IOException {
        String url = String.format("%s/%s", FTP_MASSIVE, accession);
        URI uri = UriUtils.toUri(url);
        try (DdiFPTClient ftpClient = createFtpClient(uri.getHost(), "anonymous", "anonymous")) {
            return FtpUtils.getListFiles(ftpClient, uri.getPath()).stream()
                    .map(x -> String.format("ftp://%s%s", uri.getHost(), x))
                    .collect(Collectors.toSet());
        }
    }

    @Override
    protected boolean isSupported(String database) {
        return database.equals(DB.MASSIVE.getDBName()) || database.equals(DB.MASSIVE_OLD.getDBName());
    }
}
