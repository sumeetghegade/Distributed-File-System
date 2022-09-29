package nameServer;

import java.util.HashSet;

public class FileVersionEntry {

    long size;
    boolean isTombStone;
    HashSet<String> hostPorts;


    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public boolean isTombStone() {
        return isTombStone;
    }

    public void setTombStone(boolean tombStone) {
        isTombStone = tombStone;
    }

    public HashSet<String> getHostPorts() {
        return hostPorts;
    }

    public void setHostPorts(HashSet<String> hostPorts) {
        this.hostPorts = hostPorts;
    }

    public void addHostPorts(String hostPort) {
        this.hostPorts.add(hostPort);
    }

    public void removeHostPort(String hostPort) {
        this.hostPorts.remove(hostPort);
    }
}
