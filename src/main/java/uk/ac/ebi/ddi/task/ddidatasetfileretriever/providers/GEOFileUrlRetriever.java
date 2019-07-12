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

public class GEOFileUrlRetriever extends DatasetFileUrlRetriever {

    private static final String FTP_GEO = "ftp://ftp.ncbi.nlm.nih.gov/geo";

    public GEOFileUrlRetriever(IDatasetFileUrlRetriever datasetDownloadingRetriever) {
        super(datasetDownloadingRetriever);
    }

    @Override
    public Set<String> getAllDatasetFiles(String accession, String database) throws IOException {
        String url = FTP_GEO;
        if (isSerial(accession)) {
            url += "/series";
        } else if (isDataset(accession)) {
            url += "/datasets";
        } else {
            throw new RuntimeException("GEO accession not found: " + accession);
        }
        url += "/" + accession.substring(0, accession.length() - 3) + "nnn";
        url += "/" + accession;
        url += "/suppl";
        URI uri = UriUtils.toUri(url);
        try (DdiFPTClient ftpClient = createFtpClient(uri.getHost(), "anonymous", "anonymous")) {
            return FtpUtils.getListFiles(ftpClient, uri.getPath()).stream()
                    .map(x -> String.format("ftp://%s%s", uri.getHost(), x))
                    .collect(Collectors.toSet());
        }
    }

    @Override
    protected boolean isSupported(String database) {
        return database.equals(DB.GEO.getDBName());
    }

    private boolean isSerial(String accession) {
        return accession.contains("GSE");
    }

    private boolean isDataset(String accession) {
        return accession.contains("GDS");
    }
}
