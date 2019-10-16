package top.thinkin.lightd.raft;

import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import org.apache.commons.io.FileUtils;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.exception.KitDBException;

import java.io.File;
import java.io.IOException;

public class LightDServer {
    // jraft 服务端服务框架
    private RaftGroupService raftGroupService;

    // raft 节点
    private Node node;

    // 数据库状态机
    private DBStateMachine dbsm;


    public LightDServer(String dataPath, String groupId, PeerId serverId, NodeOptions nodeOptions, DB db) throws IOException, KitDBException {
        FileUtils.forceMkdir(new File(dataPath));
        this.dbsm = new DBStateMachine();
        this.dbsm.setDB(db);
        RpcServer rpcServer = new RpcServer(serverId.getPort());
        RaftRpcServerFactory.addRaftRequestProcessors(rpcServer);

        dbsm.setDbRequestProcessor(new DBRequestProcessor(this));

        nodeOptions.setFsm(this.dbsm);
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        // 元信息, 必须
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        // snapshot, 可选, 一般都推荐
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // 初始化 raft group 服务框架
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer);
        // 启动
        this.node = this.raftGroupService.start();


    }


    public DBStateMachine getFsm() {
        return this.dbsm;
    }

    public Node getNode() {
        return this.node;
    }

    public RaftGroupService RaftGroupService() {
        return this.raftGroupService;
    }


    public static void main(String[] args) {

    }
}
