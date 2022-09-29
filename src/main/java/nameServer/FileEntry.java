package nameServer;

import java.util.HashMap;

public class FileEntry {

    //ToDo: Remove fileName?
    String fileName;

    HashMap<Integer, FileVersionEntry> fileVersions;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public HashMap<Integer, FileVersionEntry> getFileVersions() {
        return fileVersions;
    }

    public void setFileVersions(HashMap<Integer, FileVersionEntry> fileVersions) {
        this.fileVersions = fileVersions;
    }


    public void putFileVersions(int fileVersion, FileVersionEntry fVersion) {
        this.fileVersions.put(fileVersion, fVersion);
    }
}
