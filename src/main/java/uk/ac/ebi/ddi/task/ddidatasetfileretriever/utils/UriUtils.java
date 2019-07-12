package uk.ac.ebi.ddi.task.ddidatasetfileretriever.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class UriUtils {

    public static URI toUri(String url) throws IOException {
        try {
            return new URI(url);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }
}
