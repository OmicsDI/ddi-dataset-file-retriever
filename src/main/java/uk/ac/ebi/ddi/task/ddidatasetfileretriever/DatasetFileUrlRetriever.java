package uk.ac.ebi.ddi.task.ddidatasetfileretriever;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.HttpClients;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.utils.DdiFPTClient;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.utils.RetryClient;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Set;

public abstract class DatasetFileUrlRetriever extends RetryClient implements IDatasetFileUrlRetriever {

    private IDatasetFileUrlRetriever datasetDownloadingRetriever;

    protected RestTemplate restTemplate;

    private static final int FTP_TIMEOUT = 5 * 60000;

    private static final int BUFFER_LIMIT = 1024 * 1024;

    public DatasetFileUrlRetriever(IDatasetFileUrlRetriever datasetDownloadingRetriever) {
        this.datasetDownloadingRetriever = datasetDownloadingRetriever;
        try {
            this.restTemplate = new RestTemplate(clientHttpRequestFactory());
        } catch (KeyStoreException | NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setSupportedMediaTypes(Collections.singletonList(MediaType.TEXT_HTML));
        restTemplate.getMessageConverters().add(converter);
    }

    protected abstract Set<String> getAllDatasetFiles(String accession, String database) throws IOException;

    protected abstract boolean isSupported(String database);

    protected DdiFPTClient createFtpClient(String host, String username, String password) throws IOException {
        DdiFPTClient ftpClient = new DdiFPTClient();
        ftpClient.setConnectTimeout(FTP_TIMEOUT);
        ftpClient.setDefaultTimeout(FTP_TIMEOUT);
        ftpClient.connect(host);
        ftpClient.login(username, password);
        ftpClient.setBufferSize(BUFFER_LIMIT);
        ftpClient.enterLocalPassiveMode();
        return ftpClient;
    }

    @Override
    public Set<String> getDatasetFiles(String accession, String database) throws IOException {
        Set<String> result = datasetDownloadingRetriever.getDatasetFiles(accession, database);
        if (isSupported(database)) {
            result.addAll(getAllDatasetFiles(accession, database));
        }
        return result;
    }

    private ClientHttpRequestFactory clientHttpRequestFactory()
            throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;

        SSLContext sslContext = org.apache.http.ssl.SSLContexts.custom()
                .loadTrustMaterial(null, acceptingTrustStrategy)
                .build();

        SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext);
        factory.setReadTimeout(200000);
        factory.setConnectTimeout(200000);
        HttpClient httpClient = HttpClients.custom()
                .setSSLSocketFactory(csf)
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();
        factory.setHttpClient(httpClient);
        return factory;
    }
}
