package nameServer;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.quorum.Leader;
import qddfs.FileStoreGrpc;
import qddfs.NameServerGrpc;
import qddfs.Qddfs1;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class NameServerImpl extends NameServerGrpc.NameServerImplBase {


    //Task to remove handle inactive filestores
    public static TimerTask tt = new TimerTask() {
        @Override
        public void run() {
            for (Map.Entry<String, Instant> entry : NameServer.activeFileStores.entrySet()) {
                Duration timeElapsed = Duration.between(entry.getValue(), Instant.now());
                if (timeElapsed.toMillis() >= 30000) {
                    String hostPort = entry.getKey();
                    System.out.println("FileStore inactive: " + hostPort);
                    NameServer.activeFileStores.remove(hostPort);
                    for (Map.Entry<String, FileEntry> fEntry : NameServer.metaData.entrySet()) {
                        Map<Integer, FileVersionEntry> versionEntries = fEntry.getValue().getFileVersions();
                        if (versionEntries.isEmpty()) {
                            NameServer.metaData.remove(fEntry.getKey());
                        }
                        for (int version : versionEntries.keySet()) {
                            FileVersionEntry currEntry = versionEntries.get(version);
                            if (currEntry.getHostPorts().contains(hostPort)) {
                                currEntry.removeHostPort(hostPort);
                                if (currEntry.getHostPorts().isEmpty()) {
                                    versionEntries.remove(version);
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    public static TimerTask tt2 = new TimerTask() {
        @Override
        public void run() {
            if (NameServer.versionCount > 10) {
                //bumpVersion
                List<String> fileList = new ArrayList<>();
                for (Map.Entry<String, FileEntry> entry : NameServer.metaData.entrySet()) {
                    Map<Integer, FileVersionEntry> fsEntry = entry.getValue().getFileVersions();
                    int maxVer = Collections.max(fsEntry.keySet());
                    if (!fsEntry.get(maxVer).isTombStone()) {
                        fileList.add(entry.getKey());
                    }
                }
                for (String hostPort : NameServer.activeFileStores.keySet()) {
                    ManagedChannel channel = ManagedChannelBuilder.forTarget(hostPort).usePlaintext().build();

                    FileStoreGrpc.FileStoreBlockingStub fileStore = FileStoreGrpc.newBlockingStub(channel);
                    Qddfs1.BumpVersionRequest request = Qddfs1.BumpVersionRequest.newBuilder().addAllName(fileList).setNewVersion(NameServer.bumpVer).build();
                    fileStore.bumpVersion(request);
                }
                try {
                    byte[] newMaxVerBytearr = Integer.toString(NameServer.bumpVer).getBytes(StandardCharsets.UTF_8);
                    NameServer.zk.setData(FileSystemConstants.ZK_CP + "/minver", newMaxVerBytearr, NameServer.zk.exists(FileSystemConstants.ZK_CP + "/minver", true).getVersion());
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                NameServer.bumpVer = NameServer.currVer;
                NameServer.versionCount = 0;
            }
        }
    };


    @Override
    public void doCreate(Qddfs1.NSCreateRequest request, StreamObserver<Qddfs1.NSCreateReply> responseObserver) {

        System.out.println("Received doCreate request");
        Qddfs1.NSCreateReply reply;

        //1. Check if I am NameServer or waiting, if not return with rc = 1.
        if (NameServer.getState() != NameServer.NameServerStates.ELECTED || NameServer.canServe == false) {
            System.out.println("I am not the NameServer, returning rc = 2");
            reply = Qddfs1.NSCreateReply.newBuilder().setRc(2).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }

        List<String> hostPorts = new ArrayList<>();
        int count = 0;
        Iterator<String> itr = NameServer.activeFileStores.keySet().iterator();
        while (itr.hasNext() && count < 3) {
            hostPorts.add(itr.next());
            count++;
        }
        NameServer.currVer++;
        NameServer.versionCount++;
        if (NameServer.currVer > NameServer.nextMaxVer) {

            int n = NameServer.nextMaxVer + 10;
            NameServer.nextMaxVer = n;
            byte[] newMaxVerBytearr = Integer.toString(n).getBytes(StandardCharsets.UTF_8);
            try {
                System.out.println("Changing /maxver value to: " + NameServer.nextMaxVer);
                NameServer.zk.setData(FileSystemConstants.ZK_CP + "/maxver", newMaxVerBytearr, NameServer.zk.exists(FileSystemConstants.ZK_CP + "/maxver", true).getVersion());
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        responseObserver.onNext(Qddfs1.NSCreateReply.newBuilder().addAllHostPort(hostPorts).setVersion(NameServer.currVer).setRc(0).build());
        responseObserver.onCompleted();
    }

    @Override
    public void doRead(Qddfs1.NSReadRequest request, StreamObserver<Qddfs1.NSReadReply> responseObserver) {

        System.out.println("Received doRead request");
        Qddfs1.NSReadReply reply;

        //1. Check if I am NameServer or waiting, if not return with rc = 1.
        if (NameServer.getState() != NameServer.NameServerStates.ELECTED || NameServer.canServe == false) {
            System.out.println("I am not the NameServer, returning rc = 2");
            reply = Qddfs1.NSReadReply.newBuilder().setRc(2).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }

        String fileName = request.getName();
        System.out.println("Received request for file: " + fileName);

        if (NameServer.metaData.containsKey(fileName)) {
            System.out.println("File exists in metadata, getting info");
            HashMap<Integer, FileVersionEntry> fEntry = NameServer.metaData.get(fileName).getFileVersions();
            int maxver = Collections.max(fEntry.keySet());
            if (!fEntry.get(maxver).isTombStone()) {
                FileVersionEntry fVersionEntry = fEntry.get(maxver);
                reply = Qddfs1.NSReadReply.newBuilder().addAllHostPort(fVersionEntry.getHostPorts()).setRc(0).build();
            } else {
                System.out.println("File has been deleted");
                reply = Qddfs1.NSReadReply.newBuilder().addAllHostPort(null).setRc(1).build();
            }
        } else {
            System.out.println("File does not exist in metadata");
            reply = Qddfs1.NSReadReply.newBuilder().setRc(1).build();
        }

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void doDelete(Qddfs1.NSDeleteRequest request, StreamObserver<Qddfs1.NSDeleteReply> responseObserver) {

        System.out.println("Received doDelete request");
        Qddfs1.NSDeleteReply deleteReply;
        //1. Check if I am NameServer or waiting, if not return with rc = 1.
        if (NameServer.getState() != NameServer.NameServerStates.ELECTED || NameServer.canServe == false) {
            System.out.println("I am not the NameServer, returning rc = 1");
            deleteReply = Qddfs1.NSDeleteReply.newBuilder().setRc(1).build();
            responseObserver.onNext(deleteReply);
            responseObserver.onCompleted();
            return;
        }

        String fileName = request.getName();
        ManagedChannel channel = null;

        if (NameServer.metaData.containsKey(fileName)) {
            System.out.println("File entry exists in metadata");
            Map<Integer, FileVersionEntry> versions = NameServer.metaData.get(fileName).getFileVersions();
            int maxVersion = Collections.max(versions.keySet());
            if (!versions.get(maxVersion).isTombStone()) {
                System.out.println("File has not been deleted");
                FileVersionEntry version = versions.get(maxVersion);
                for (String hostPort : version.getHostPorts()) {
                    channel = ManagedChannelBuilder.forTarget(hostPort).usePlaintext().build();
                    FileStoreGrpc.FileStoreBlockingStub fileStore = FileStoreGrpc.newBlockingStub(channel);

                    Qddfs1.DeleteFileRequest deleteRequest = Qddfs1.DeleteFileRequest.newBuilder().setName(fileName)
                            .setVersion(maxVersion).build();

                    fileStore.deleteFile(deleteRequest);
                }
                deleteReply = Qddfs1.NSDeleteReply.newBuilder().setRc(0).build();
                if (channel != null)
                    channel.shutdown();
            } else {
                deleteReply = Qddfs1.NSDeleteReply.newBuilder().setRc(1).build();
            }
        } else {
            deleteReply = Qddfs1.NSDeleteReply.newBuilder().setRc(1).build();
        }
        responseObserver.onNext(deleteReply);
        responseObserver.onCompleted();
    }

    @Override
    public void list(Qddfs1.NSListRequest request, StreamObserver<Qddfs1.NSListReply> responseObserver) {

        System.out.println("Received list request");
        //1. Check if I am NameServer or waiting, if not return with rc = 2.
        if (NameServer.getState() != NameServer.NameServerStates.ELECTED || NameServer.canServe == false) {
            System.out.println("I am not the NameServer, returning rc = 2");
            Qddfs1.NSListReply reply = Qddfs1.NSListReply.newBuilder().setRc(2).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }

        try {
            String pattern = request.getPattern();
            System.out.println("Received pattern: " + pattern);
            List<Qddfs1.NSNameEntry> nsEntries = new ArrayList<>();
            for (String fileName : NameServer.metaData.keySet()) {
                if (fileName.matches(pattern)) {
                    FileEntry fEntry = NameServer.metaData.get(fileName);
                    int maxVer = Collections.max(fEntry.getFileVersions().keySet());

                    FileVersionEntry version = fEntry.getFileVersions().get(maxVer);
                    if (!version.isTombStone()) {
                        nsEntries.add(Qddfs1.NSNameEntry.newBuilder().setName(fileName).setSize(version.getSize()).setVersion(maxVer).build());
                    }

                }
            }
            Qddfs1.NSListReply reply = Qddfs1.NSListReply.newBuilder().setRc(0).addAllEntries(nsEntries).build();
            responseObserver.onNext(reply);
        } catch (Exception e) {
            e.printStackTrace();
            Qddfs1.NSListReply reply = Qddfs1.NSListReply.newBuilder().setRc(2).addAllEntries(null).build();
            responseObserver.onNext(reply);
        } finally {
            responseObserver.onCompleted();
        }


    }

    @Override
    public void registerFilesAndTombstones(Qddfs1.NSRegisterRequest request, StreamObserver<Qddfs1.NSRegisterReply> responseObserver) {

        System.out.println("Received registerFilesAndTombstones from:  " + request.getHostPort());

        //1. Check if I am NameServer or waiting, if not return with rc = 2.
        if (NameServer.getState() != NameServer.NameServerStates.ELECTED) {
            System.out.println("I am not the NameServer, returning rc = 2");
            Qddfs1.NSRegisterReply reply = Qddfs1.NSRegisterReply.newBuilder().setRc(2).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }

        //2. Iterate over all file entries and add to metadata.
        List<Qddfs1.FSEntry> entryList = request.getEntriesList();
        for (Qddfs1.FSEntry entry : entryList) {
            String fileName = entry.getName();
            boolean isTombstone = entry.getIsTombstone();
            int version = entry.getVersion();
            System.out.println("Received data -> isTombstone: " + isTombstone + "version: " + version);
            if (NameServer.metaData.containsKey(fileName)) {
                System.out.println("File already exists");
                FileEntry fileInfo = NameServer.metaData.get(fileName);

                if (fileInfo.getFileVersions().containsKey(version)) {
                    System.out.println("Version already exists, adding hostport to the set");
                    fileInfo.getFileVersions().get(version).addHostPorts(request.getHostPort());
                } else {
                    System.out.println("New file version, adding to metadata");
                    FileVersionEntry fv = new FileVersionEntry();
                    fv.setTombStone(isTombstone);
                    fv.setSize(entry.getSize());
                    HashSet<String> hps = new HashSet<String>();
                    hps.add(request.getHostPort());
                    fv.setHostPorts(hps);
                    fileInfo.putFileVersions(version, fv);
                }

            } else {
                System.out.println("New file entry, adding in metadata");
                FileEntry fEntry = new FileEntry();
                HashMap<Integer, FileVersionEntry> fVersion = new HashMap<>();
                HashSet<String> hostPorts = new HashSet<>();
                hostPorts.add(request.getHostPort());
                FileVersionEntry fVersionEntry = new FileVersionEntry();
                fVersionEntry.setHostPorts(hostPorts);
                fVersionEntry.setTombStone(isTombstone);
                fVersionEntry.setSize(entry.getSize());
                fVersion.put(version, fVersionEntry);
                fEntry.setFileVersions(fVersion);
                NameServer.metaData.put(fileName, fEntry);
            }
        }
        System.out.println("Entries added to metadata, returning rc=0");
        Qddfs1.NSRegisterReply reply = Qddfs1.NSRegisterReply.newBuilder().setRc(0).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void addFileOrTombstone(Qddfs1.NSAddRequest request, StreamObserver<Qddfs1.NSAddReply> responseObserver) {

        System.out.println("Received addFileOrTombstone from:  " + request.getHostPort());
        //1. Check if I am NameServer or waiting, if not return with rc = 2.
        if (NameServer.getState() != NameServer.NameServerStates.ELECTED || NameServer.canServe == false) {
            System.out.println("I am not the NameServer, returning rc = 2");
            Qddfs1.NSAddReply reply = Qddfs1.NSAddReply.newBuilder().setRc(2).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }

        Qddfs1.FSEntry entry = request.getEntry();
        String fileName = entry.getName();
        boolean isTombstone = entry.getIsTombstone();
        int version = entry.getVersion();
        System.out.println("Received data -> isTombstone: " + isTombstone + "version: " + version);
        if (NameServer.metaData.containsKey(fileName)) {
            System.out.println("File already exists");
            FileEntry fileInfo = NameServer.metaData.get(fileName);

            if (fileInfo.getFileVersions().containsKey(version)) {
                System.out.println("Version already exists, adding hostport to the set");
                fileInfo.getFileVersions().get(version).addHostPorts(request.getHostPort());
            } else {
                System.out.println("New file version, adding to metadata");
                FileVersionEntry fv = new FileVersionEntry();
                fv.setTombStone(isTombstone);
                fv.setSize(entry.getSize());
                HashSet<String> hps = new HashSet<String>();
                hps.add(request.getHostPort());
                fv.setHostPorts(hps);
                fileInfo.putFileVersions(version, fv);
            }

        } else {
            System.out.println("New file entry, adding in metadata");
            FileEntry fEntry = new FileEntry();
            HashMap<Integer, FileVersionEntry> fVersion = new HashMap<>();
            HashSet<String> hostPorts = new HashSet<>();
            hostPorts.add(request.getHostPort());
            FileVersionEntry fVersionEntry = new FileVersionEntry();
            fVersionEntry.setHostPorts(hostPorts);
            fVersionEntry.setTombStone(isTombstone);
            fVersionEntry.setSize(entry.getSize());
            fVersion.put(version, fVersionEntry);
            fEntry.setFileVersions(fVersion);
            NameServer.metaData.put(fileName, fEntry);
        }

        System.out.println("Entries added to metadata, returning rc=0");
        Qddfs1.NSAddReply reply = Qddfs1.NSAddReply.newBuilder().setRc(0).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void heartBeat(Qddfs1.NSBeatRequest request, StreamObserver<Qddfs1.NSBeatReply> responseObserver) {

        String hostport = request.getHostPort();
        System.out.println("Received heartBeat from: " + hostport);
        NameServer.activeFileStores.put(hostport, Instant.now());

        responseObserver.onNext(Qddfs1.NSBeatReply.newBuilder().setRc(0).build());
        responseObserver.onCompleted();

    }
}
