package uk.ac.ebi.ddi.task.ddidatasetfileretriever.utils;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FtpUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(FtpUtils.class);

    private FtpUtils() {
    }

    /**
     * Get list of files from given path that in their name or inside their path must not contain the given string
     * @param client FTPClient
     * @param path path of the folder
     * @param ignoreDir list of dir name to be ignored
     * @return list of file paths
     * @throws IOException
     */
    public static List<String> getListFiles(FTPClient client, String path, String... ignoreDir) throws IOException {
        List<String> result = new ArrayList<>();
        for (String dir : ignoreDir) {
            if (path.equals(dir)) {
                return result;
            }
        }
        if (!client.changeWorkingDirectory(path)) {
            return Collections.emptyList();
        }
        FTPFile[] ftpFiles = client.listFiles();
        for (FTPFile file : ftpFiles) {
            if (!file.getName().equals(".") && !file.getName().equals("..")) {
                if (file.isDirectory()) {
                    result.addAll(getListFiles(client, file.getName(), ignoreDir));
                } else {
                    String link = String.format("%s/%s", client.printWorkingDirectory(), file.getName());
                    result.add(link);
                }
            }
        }
        client.changeToParentDirectory();
        return result;
    }


    /**
     * Get list of files from given path that in their name or inside their path have to contain the given string
     *
     * @param client FTPClient
     * @param path path of the folder
     * @param mustHave must have string
     * @return list of paths
     * @throws IOException
     */
    public static List<String> getListFilesContaining(FTPClient client, String path, String mustHave)
            throws IOException {
        List<String> result = new ArrayList<>();
        if (!client.changeWorkingDirectory(path)) {
            return Collections.emptyList();
        }
        FTPFile[] ftpFiles = client.listFiles();
        for (FTPFile file : ftpFiles) {
            if (!file.getName().equals(".") && !file.getName().equals("..") && file.getName().contains(mustHave)) {
                if (file.isDirectory()) {
                    result.addAll(getListFilesContaining(client, file.getName(), mustHave));
                } else {
                    String link = String.format("%s/%s", client.printWorkingDirectory(), file.getName());
                    result.add(link);
                }
            }
        }
        client.changeToParentDirectory();
        return result;
    }
}
