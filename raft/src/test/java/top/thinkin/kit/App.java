package top.thinkin.kit;

import com.alipay.sofa.jraft.RouteTable;
import fi.iki.elonen.NanoHTTPD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.db.ZSet;
import top.thinkin.lightd.exception.KitDBException;
import top.thinkin.lightd.raft.DBRequestProcessor;
import top.thinkin.lightd.raft.GroupConfig;
import top.thinkin.lightd.raft.KitRaft;
import top.thinkin.lightd.raft.NodeConfig;

import java.io.IOException;
import java.util.Map;

public class App extends NanoHTTPD {
    public App(int port) throws IOException {
        super(port);
        start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
        System.out.println("\nRunning! Point your browsers to http://localhost:8080/ \n");
    }

    private static final Logger LOG = LoggerFactory.getLogger(DBRequestProcessor.class);


    private KitRaft kitRaft;

    private static RouteTable rt;


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

        // 解析参数

        // 设置初始集群配置
        DB db = DB.build("D:\\temp\\" + dnName, false);

        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setGroup("test");
        groupConfig.setInitNodes(initConfStr);


        NodeConfig nodeConfig = new NodeConfig();
        nodeConfig.setNode(serverIdStr);
        nodeConfig.setRaftDir(dataPath);

        KitRaft counterServer = new KitRaft(groupConfig, nodeConfig, db);

        try {
            App app = new App(Integer.parseInt(portStr));
            app.kitRaft = counterServer;
        } catch (IOException ioe) {
            System.err.println("Couldn't start server:\n" + ioe);
        }

        rt = RouteTable.getInstance();
    }

    public Response serve(IHTTPSession session) {
        String msg = "<html><body><h1>Hello server</h1>\n";

        if ("/w/".equals(session.getUri())) {
            Map<String, String> parms = session.getParms();
            String m = parms.get("m");
            String s = parms.get("s");
            ZSet zset = this.kitRaft.getDB().getzSet();
            try {
                zset.add("text", m.getBytes(), Long.parseLong(s));

            } catch (KitDBException e) {
                e.printStackTrace();
            }
            return newFixedLengthResponse("</body></html>\n");
        } else if ("/r/".equals(session.getUri())) {
            ZSet zset = this.kitRaft.getDB().getzSet();
            Map<String, String> parms = session.getParms();
            String m = parms.get("m");
            try {

                return newFixedLengthResponse("</body>" + zset.score("text", m.getBytes()) + "</html>\n");
            } catch (KitDBException e) {
                e.printStackTrace();
            }
        } else if ("/getLeader".equals(session.getUri())) {
            try {
                return newFixedLengthResponse(kitRaft.getLeader());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if ("/addPeer".equals(session.getUri())) {
            Map<String, String> parms = session.getParms();
            String node = parms.get("node");

            kitRaft.addNode(node);
        }
        return newFixedLengthResponse("</body></html>\n");

    }
}
