package top.thinkin.lightd.raft;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import fi.iki.elonen.NanoHTTPD;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.db.RKv;
import top.thinkin.lightd.exception.KitDBException;

import java.io.IOException;
import java.util.Map;

public class App extends NanoHTTPD {
    public App(int port) throws IOException {
        super(8582);
        start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
        System.out.println("\nRunning! Point your browsers to http://localhost:8080/ \n");
    }

    private LightDServer lightDServer;
    private DB db;

    public static void main(String[] args) throws IOException, KitDBException {

        if (args.length != 6) {
            System.out
                    .println("Useage : java com.alipay.jraft.example.counter.CounterServer {dataPath} {groupId} {serverId} {initConf}");
            System.out
                    .println("Example: java com.alipay.jraft.example.counter.CounterServer /tmp/server1 counter 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            System.exit(1);
        }
        String dataPath = args[0];
        String groupId = args[1];
        String serverIdStr = args[2];
        String initConfStr = args[3];
        String dnName = args[4];
        String portStr = args[5];

        NodeOptions nodeOptions = new NodeOptions();
        // 为了测试, 调整 snapshot 间隔等参数
        nodeOptions.setElectionTimeoutMs(5000);
        nodeOptions.setDisableCli(false);
        nodeOptions.setSnapshotIntervalSecs(30);
// 解析参数
        PeerId serverId = new PeerId();
        if (!serverId.parse(serverIdStr)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr);
        }
        Configuration initConf = new Configuration();
        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        // 设置初始集群配置
        nodeOptions.setInitialConf(initConf);
        DB db = DB.build("D:\\temp\\" + dnName, true);

        LightDServer counterServer = new LightDServer(dataPath, groupId, serverId, nodeOptions, db);

        try {
            App app = new App(Integer.parseInt(portStr));
            app.lightDServer = counterServer;
            app.db = db;
        } catch (IOException ioe) {
            System.err.println("Couldn't start server:\n" + ioe);
        }
    }

    @Override
    public Response serve(IHTTPSession session) {
        String msg = "<html><body><h1>Hello server</h1>\n";
        Map<String, String> parms = session.getParms();
        String k = parms.get("k");
        String v = parms.get("v");
        RKv rkv = this.db.getrKv();
        try {
            rkv.set(k, v.getBytes());
        } catch (KitDBException e) {
            e.printStackTrace();
        }

        return newFixedLengthResponse("</body></html>\n");
    }
}
