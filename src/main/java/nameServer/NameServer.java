package nameServer;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Refered from: https://learning.oreilly.com/library/view/zookeeper/9781449361297/ch03.html#idm140361989607488
 *
 * */
public class NameServer implements Watcher {
    private static final int PORT = 9098;

    public static int currVer;
    public static boolean canServe = false;
    protected static ConcurrentHashMap<String, FileEntry> metaData = new ConcurrentHashMap<String, FileEntry>();
    protected static ConcurrentHashMap<String, Instant> activeFileStores = new ConcurrentHashMap<>();
    private static volatile NameServerStates state = NameServerStates.RUNNING;
    private final String hostPort = "172.24.9.201:2181";
    private final String zkData = "172.24.9.118:9098" + "\nSumeetGhegade";
    public static ZooKeeper zk;
    private volatile boolean connected = false;
    public static int versionCount = 0;
    private volatile boolean expired = false;
    public static int nextMaxVer;
    public static int bumpVer;

    static NameServerStates getState() {
        return state;
    }

    public static void main(String[] args) throws Exception {
        NameServer ns = new NameServer();
        //Start Zookeeper session
        ns.startZK();
        while (!ns.isConnected()) {
            Thread.sleep(100);
        }
        /*
         * now run for master.
         */
        ns.runForMaster();

        //Start gRPC server for NameServer
        Server server = ServerBuilder.forPort(PORT).addService(new NameServerImpl()).build();
        try {

            server.start();
            server.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }

        while (!ns.isExpired()) {
            Thread.sleep(1000);
        }
        ns.stopZK();
    }

    void startZK() throws IOException {
        System.out.println("Starting zookeeper");
        zk = new ZooKeeper(hostPort, 15000, this);
    }


    void stopZK() throws InterruptedException, IOException {
        System.out.println("Stoping zookeeper");
        zk.close();
    }


    public void process(WatchedEvent e) {
        if (e.getType() == Event.EventType.None) {
            switch (e.getState()) {
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                default:
                    break;
            }
        }
    }



    boolean isConnected() {
        return connected;
    }


    boolean isExpired() {
        return expired;
    }


    void masterExists() {
        zk.exists(FileSystemConstants.ZK_CP + "leader",
                masterExistsWatcher,
                masterExistsCallback,
                null);
    }

    void takeLeadership() {
        System.out.println("Taking leadership");

        //1. Get data in /maxver
        try {
            byte[] maxVerData = zk.getData(FileSystemConstants.ZK_CP + "/maxver", true, null);
            currVer = Integer.parseInt(new String(maxVerData, StandardCharsets.UTF_8));
            bumpVer = currVer;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //2. Set data in /maxver to current value + 10
        nextMaxVer = currVer + 10;
        byte[] newMaxVerBytearr = Integer.toString(nextMaxVer).getBytes(StandardCharsets.UTF_8);

        try {
            zk.setData(FileSystemConstants.ZK_CP +"/maxver", newMaxVerBytearr, zk.exists(FileSystemConstants.ZK_CP + "/maxver", true).getVersion());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        //3. Wait 30secs for filestores to register
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //3. Now we can start serving requests
        canServe = true;
        Timer timer1 = new Timer();
        timer1.schedule(NameServerImpl.tt, 0, 1000);

        Timer timer2 = new Timer();
        timer2.schedule(NameServerImpl.tt2, 120000, 60000);


    }    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();

                    break;
                case OK:
                    state = NameServerStates.ELECTED;
                    takeLeadership();

                    break;
                case NODEEXISTS:
                    state = NameServerStates.NOTELECTED;
                    masterExists();

                    break;
                default:
                    state = NameServerStates.NOTELECTED;

            }
        }
    };

    public void runForMaster() {
        System.out.println("Running for master");
        zk.create(FileSystemConstants.ZK_CP + "/leader",
                zkData.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null);
    }

    void checkMaster() {
        System.out.println("Checking Master");
        zk.getData(FileSystemConstants.ZK_CP + "leader", false, masterCheckCallback, null);
    }

    enum NameServerStates {RUNNING, ELECTED, NOTELECTED}    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();

                    break;
                case OK:
                    break;
                case NONODE:
                    state = NameServerStates.RUNNING;
                    runForMaster();

                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };



    Watcher masterExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == Event.EventType.NodeDeleted) {
                assert (FileSystemConstants.ZK_CP + "leader").equals(e.getPath());

                runForMaster();
            }
        }
    };


    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();

                    break;
                case NONODE:
                    runForMaster();

                    break;
                case OK:
                    if (zkData.equals(new String(data))) {
                        state = NameServerStates.ELECTED;
                        takeLeadership();
                    } else {
                        state = NameServerStates.NOTELECTED;
                        masterExists();
                    }

                    break;
                default:
            }
        }
    };


}
