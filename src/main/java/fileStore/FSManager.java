package fileStore;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import nameServer.FileEntry;
import nameServer.FileVersionEntry;
import nameServer.NameServer;
import nameServer.NameServerImpl;
import org.apache.zookeeper.*;
import qddfs.NameServerGrpc;
import qddfs.Qddfs1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class FSManager {

    final public static String ipAddr = "172.24.9.118:9098";
    final static String dataLoc = "D:\\SJSU\\Sem-1\\CS249-DistributedComputing\\Assignments\\Assignment4_Phase2\\src\\main\\resources\\dataStore\\";
    final static String metaDataFileName = "metadata.properties";
    final static String deletedFilesDataFileName = "deletedFilesData.properties";
    final static String fileSizeDataFileName = "fileSizeData.properties";
    final static int PORT = 9098;
    public static String nsHostPort;
    public static boolean isNSAvailable;
    public static NameServerGrpc.NameServerBlockingStub nameServer = null;
    static Map<String, String> metaData = new HashMap<String, String>();
    static Map<String, String> deletedFilesData = new HashMap<String, String>();
    static Map<String, String> fileSizeData = new HashMap<String, String>();


    public static TimerTask tt = new TimerTask() {
        @Override
        public void run() {
            if (isNSAvailable)
                sendHeartBeat();
        }
    };




    public static void main(String[] args) {
        String zk = FileSystemConstants.ZK_HOSTPORT;
        String controlPath = FileSystemConstants.ZK_PATH;

        CountDownLatch connectionLatch = new CountDownLatch(1);
        ZooKeeper zookeeper = null;
        try {
            zookeeper = new ZooKeeper(zk, 1000, new Watcher() {
                @Override
                public void process(WatchedEvent we) {

                    if (we.getState() == Event.KeeperState.SyncConnected) {
                        connectionLatch.countDown();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            connectionLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            String reply = zookeeper.create(controlPath + "/SumeetGhegade", (ipAddr + "\nSumeetGhegade").getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        //1. Create metadata file
        File metaFile = new File(dataLoc + metaDataFileName);
        if (!metaFile.exists()) {
            try {
                metaFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(FSManager.dataLoc + metaDataFileName));
            } catch (IOException e) {
                e.printStackTrace();
            }

            //3.2 If metadata is not empty, get metadata
            if (!properties.isEmpty()) {
                for (String key : properties.stringPropertyNames()) {
                    metaData.put(key, properties.getProperty(key));
                }
            } else {
                //LOGGER.log(Level.INFO, "Datastore is empty");
            }
        }


        //2. Create deleted files metadata file
        File deleteFile = new File(dataLoc + deletedFilesDataFileName);
        if (!deleteFile.exists()) {
            try {
                deleteFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(FSManager.dataLoc + deletedFilesDataFileName));
            } catch (IOException e) {
                e.printStackTrace();
            }

            //3.2 If metadata is not empty, get metadata
            if (!properties.isEmpty()) {
                for (String key : properties.stringPropertyNames()) {
                    deletedFilesData.put(key, properties.getProperty(key));
                }
            } else {
                System.out.println("Datastore is empty");
            }
        }

        //3. Create file size data file
        File fileSizeDataFile = new File(dataLoc + fileSizeDataFileName);
        if (!fileSizeDataFile.exists()) {
            try {
                fileSizeDataFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(FSManager.dataLoc + fileSizeDataFileName));
            } catch (IOException e) {
                e.printStackTrace();
            }

            //3.2 If metadata is not empty, get metadata
            if (!properties.isEmpty()) {
                for (String key : properties.stringPropertyNames()) {
                    fileSizeData.put(key, properties.getProperty(key));
                }
            } else {
                //LOGGER.log(Level.INFO, "Datastore is empty");
            }
        }

        watchminver(zookeeper, FileSystemConstants.ZK_PATH + "/minver");
        getNameServerInfo(zookeeper, FileSystemConstants.ZK_PATH + "/leader");


        Timer timer = new Timer();
        timer.schedule(tt, 0, 1000);


        Server server = ServerBuilder.forPort(PORT).addService(new FileStore()).build();

        try {

            server.start();
            server.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    private static void sendHeartBeat() {
        try {
            Qddfs1.NSBeatRequest heartBeatRequest = Qddfs1.NSBeatRequest.newBuilder().setHostPort(ipAddr).setBytesAvailable(0).setBytesUsed(0).build();
            Qddfs1.NSBeatReply reply = nameServer.heartBeat(heartBeatRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void watchminver(ZooKeeper zookeeper, String controlPath) {
        System.out.println("Watching /minver");
        try {
            Watcher nsDataChangeWatcher = new Watcher() {
                @Override
                public void process(WatchedEvent e) {
                    if (e.getType() == Event.EventType.NodeDataChanged) {
                        watchminver(zookeeper, controlPath);
                    }
                }
            };
            try {
                int minver = Integer.parseInt(new String(zookeeper.getData(controlPath, nsDataChangeWatcher, null), StandardCharsets.UTF_8));
                System.out.println("new /minver: " + minver);
                performGarbageCollection(minver);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void registerFilesAndTombstones(){
        System.out.println("Registering files and tombstones");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(nsHostPort).usePlaintext().build();
        nameServer = NameServerGrpc.newBlockingStub(channel);

        List<Qddfs1.FSEntry> fsEntries = new ArrayList<>();
        for (Map.Entry<String,String> entry : metaData.entrySet()){
            fsEntries.add(Qddfs1.FSEntry.newBuilder().setName(entry.getKey()).setSize(Long.parseLong(fileSizeData.get(entry.getKey()))).setVersion(Integer.parseInt(entry.getValue())).setIsTombstone(false).build());
        }

        for (Map.Entry<String,String> entry : deletedFilesData.entrySet()){
            fsEntries.add(Qddfs1.FSEntry.newBuilder().setName(entry.getKey()).setSize(0).setVersion(Integer.parseInt(entry.getValue())).setIsTombstone(true).build());
        }

        Qddfs1.NSRegisterRequest request = Qddfs1.NSRegisterRequest.newBuilder().addAllEntries(fsEntries).setBytesAvailable(0).setBytesUsed(0).setHostPort(ipAddr).build();
        try {
            Qddfs1.NSRegisterReply reply = nameServer.registerFilesAndTombstones(request);
            if (reply.getRc() == 0) {
                System.out.println("Registered files and tombstones successfully");
            } else {
                System.out.println("Something went wrong with Registering files and tombstone");
            }
        } catch (Exception e) {
            System.out.println("Something went wrong with Registering files and tombstone");
            e.printStackTrace();
        }


    }



    public static void performGarbageCollection(int minver)
    {
        System.out.println("Starting garbage collection");
        //delete from metadata
        ArrayList<String> filesToDelete1 = new ArrayList<String>();
        for (Map.Entry<String,String> entry : metaData.entrySet())
        {
            if(Integer.parseInt(entry.getValue()) < minver)
            {
                filesToDelete1.add(entry.getKey());
                if (isNSAvailable) {
                    try {
                        Qddfs1.NSAddRequest addFileRequest = Qddfs1.NSAddRequest.newBuilder().setEntry(Qddfs1.FSEntry.newBuilder().setName(entry.getKey()).setVersion(Integer.parseInt(entry.getValue())).setSize(0).setIsTombstone(true).build()).build();
                        Qddfs1.NSAddReply reply = nameServer.addFileOrTombstone(addFileRequest);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        for (String fileName: filesToDelete1)
        {
            System.out.println("Removing file from metadata: " + fileName);
            metaData.remove(fileName);
            setMetaData();
        }

        //delete from deletedfilesdata
        ArrayList<String> filesToDelete2 = new ArrayList<String>();
        for (Map.Entry<String,String> entry : deletedFilesData.entrySet())
        {
            if(Integer.parseInt(entry.getValue()) < minver)
            {
                filesToDelete2.add(entry.getKey());
            }
        }

        for (String fileName: filesToDelete2)
        {
            System.out.println("Removing file from tombstones: " + fileName);
            deletedFilesData.remove(fileName);
            setDeletedFilesData();
        }
        System.out.println("Garbage collection done");
    }


    public static void getNameServerInfo(ZooKeeper zk, String leaderPath)
    {
        System.out.println("Getting nameserver info");
        try {
            Watcher nsDataChangeWatcher = new Watcher() {
                @Override
                public void process(WatchedEvent e) {
                    if (e.getType() == Event.EventType.NodeCreated || e.getType() == Event.EventType.NodeDataChanged || e.getType() == Event.EventType.NodeDeleted) {
                        getNameServerInfo(zk, leaderPath);
                    }
                }
            };
            try {
                String[] nsInfo = new String(zk.getData(leaderPath, nsDataChangeWatcher, null), StandardCharsets.UTF_8).split("\n", 2);
                nsHostPort = nsInfo[0];
                isNSAvailable = true;
                System.out.println("NameServer is: " + nsInfo[1] + " on hostport: " + nsHostPort);

                registerFilesAndTombstones();
            } catch (Exception e) {
                e.printStackTrace();
                nsHostPort = "";
                isNSAvailable = false;
                getNameServerInfo(zk, leaderPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
            nsHostPort = "";
            isNSAvailable = false;
            getNameServerInfo(zk, leaderPath);
        }
    }



    public static void setMetaData() {
        Properties properties = new Properties();
        properties.putAll(metaData);
        try {
            properties.store(new FileOutputStream(dataLoc + metaDataFileName), null);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void setFileSizeData() {
        Properties properties = new Properties();
        properties.putAll(fileSizeData);
        try {
            properties.store(new FileOutputStream(dataLoc + fileSizeDataFileName), null);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void setDeletedFilesData() {
        Properties properties = new Properties();
        properties.putAll(deletedFilesData);
        try {
            properties.store(new FileOutputStream(dataLoc + deletedFilesDataFileName), null);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
