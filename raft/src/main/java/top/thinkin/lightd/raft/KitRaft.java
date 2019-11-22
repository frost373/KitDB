package top.thinkin.lightd.raft;

import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.thinkin.lightd.db.DB;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KitRaft {
    private static final Logger LOG = LoggerFactory.getLogger(KitRaft.class);

    private RaftGroupService raftGroupService;

    private Node node;

    // 数据库状态机
    private DBStateMachine dbsm;


    public DB getDB() {
        return dbsm.getDb();
    }

    public KitRaft(GroupConfig groupConfig, NodeConfig nodeConfig, DB db) throws IOException {

        NodeOptions nodeOptions = new NodeOptions();

        nodeOptions.setElectionTimeoutMs(groupConfig.getElectionTimeoutMs());
        nodeOptions.setDisableCli(true);
        nodeOptions.setSnapshotIntervalSecs(groupConfig.getSnapshotIntervalSecs());

        PeerId serverId = new PeerId();
        if (!serverId.parse(nodeConfig.getNode())) {
            throw new IllegalArgumentException("Fail to parse serverId:" + nodeConfig.getNode());
        }

        Configuration initConf = new Configuration();
        if (!initConf.parse(groupConfig.getInitNodes())) {
            throw new IllegalArgumentException("Fail to parse initConf:" + groupConfig.getInitNodes());
        }

        nodeOptions.setInitialConf(initConf);

        String raftDir = nodeConfig.getRaftDir();
        FileUtils.forceMkdir(new File(raftDir));

        RpcServer rpcServer = new RpcServer(serverId.getPort());
        RaftRpcServerFactory.addRaftRequestProcessors(rpcServer);

        this.dbsm = new DBStateMachine();
        dbsm.setDbRequestProcessor(new DBRequestProcessor(this));
        dbsm.setDB(db);
        nodeOptions.setFsm(this.dbsm);

        nodeOptions.setLogUri(raftDir + File.separator + "log");
        nodeOptions.setRaftMetaUri(raftDir + File.separator + "raft_meta");
        nodeOptions.setSnapshotUri(raftDir + File.separator + "snapshot");

        this.raftGroupService = new RaftGroupService(groupConfig.getGroup(), serverId, nodeOptions, rpcServer);
        // 启动
        this.node = this.raftGroupService.start();
    }


    public void addNode(String nodeConf) {
        PeerId peer = new PeerId();
        peer.parse(nodeConf);
        node.addPeer(peer, s -> LOG.error("addNode error", s.getErrorMsg()));
    }

    public void removeNode(String nodeConf) {
        PeerId peer = new PeerId();
        peer.parse(nodeConf);
        node.removePeer(peer, s -> LOG.error("removeNode error", s.getErrorMsg()));
    }

    public void transferLeader(String nodeConf) {
        PeerId serverId = new PeerId();
        if (!serverId.parse(nodeConf)) {
            throw new IllegalArgumentException("Fail to parse Conf:" + nodeConf);
        }
        node.transferLeadershipTo(serverId);
    }

    public String getLeader() {
        PeerId peerId = node.getLeaderId();
        if (peerId == null) {
            return null;
        }
        return node.getLeaderId().toString();
    }


    public boolean isLeader() {
        return dbsm.isLeader();
    }


    public List<String> getNodes() {
        List<String> nodes = new ArrayList<>();
        List<PeerId> peerIds = node.listPeers();
        for (PeerId peerId : peerIds) {
            nodes.add(peerId.toString());
        }
        return nodes;
    }

    public List<String> getAliveNodes() {
        List<String> nodes = new ArrayList<>();
        List<PeerId> peerIds = node.listAlivePeers();
        for (PeerId peerId : peerIds) {
            nodes.add(peerId.toString());
        }
        return nodes;
    }


    public DBStateMachine getFsm() {
        return this.dbsm;
    }

    protected Node getNode() {
        return this.node;
    }

    protected RaftGroupService RaftGroupService() {
        return this.raftGroupService;
    }

}
