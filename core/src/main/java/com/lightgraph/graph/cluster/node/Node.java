package com.lightgraph.graph.cluster.node;

import com.lightgraph.graph.writable.Sizeable;
import com.lightgraph.graph.writable.Writable;
import com.lightgraph.graph.config.GraphConfig;
import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.utils.ByteUtils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;

public class Node implements Writable, Sizeable {
    private static volatile Set<String> masters = null;
    private static Node myself;
    private String name;
    private String addr;
    private Short port;
    private NodeState state;
    private NodeType type;

    static {
        GraphConfig conf = GraphConfig.getInstance();
        String addrPrefix = conf.get(GraphConstant.SERVER_ADDR_PREFIX, GraphConstant.SERVER_ADDR_PREFIX_DEFAULT);
        Short port = conf.get(GraphConstant.SERVER_PORT_NUM, GraphConstant.SERVER_PORT_NUM_DEFAULT);
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface nif = interfaces.nextElement();
                Enumeration<InetAddress> address = nif.getInetAddresses();
                while (address.hasMoreElements()) {
                    String addr = address.nextElement().getHostAddress();
                    if (addr.startsWith(addrPrefix)) {
                        myself = new Node(addr + GraphConstant.SPLIT_NET_ADDRESS_TOKEN + port, addr, port);
                        break;
                    }
                }
                if (myself != null)
                    break;
            }
            if (myself == null) {
                throw new GraphException(String.format("can not found host by prefix:%s", addrPrefix));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public Node(String name, String addr, Short port) {
        this(name, addr, port, NodeState.INIT, null);
    }

    public Node(String name, String addr, Short port, NodeState state, NodeType type) {
        if (type == null) {
            if (masters == null)
                loadMasters();
            if (masters.contains(name)) {
                type = NodeType.MASTER;
            } else {
                type = NodeType.DATA;
            }
        }
        this.name = name;
        this.addr = addr;
        this.port = port;
        this.state = state;
        this.type = type;
    }

    public Node(byte[] bytes) {
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        int nameSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        int addrSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        this.name = ByteUtils.getString(bytes, pos, nameSize);
        pos = pos + nameSize;
        this.addr = ByteUtils.getString(bytes, pos, addrSize);
        pos = pos + addrSize;
        this.port = ByteUtils.getShort(bytes, pos);
        pos = pos + ByteUtils.SIZE_SHORT;
        byte stateB = ByteUtils.getByte(bytes, pos);
        this.state = NodeState.valueOf(stateB);
        pos = pos + ByteUtils.SIZE_BYTE;
        byte typeB = ByteUtils.getByte(bytes, pos);
        this.type = NodeType.valueOf(typeB);
    }

    public void loadMasters() {
        if (masters == null) {
            synchronized (Node.class) {
                if (masters == null) {
                    Set<String> tmp = new HashSet<>();
                    GraphConfig conf = GraphConfig.getInstance();
                    String masterServers = conf.get(GraphConstant.GRAPH_MASTER_SERVERS, GraphConstant.GRAPH_MASTER_SERVERS_DEFAULT);
                    String[] masterServersArray = masterServers.split(GraphConstant.SPLIT_ARRAY_TOKEN);
                    for (String name : masterServersArray) {
                        tmp.add(name);
                    }
                    masters = tmp;
                }
            }
        }
    }

    public static Node myself() {
        return myself;
    }

    public String getName() {
        return name;
    }


    public String getAddr() {
        return addr;
    }


    public Short getPort() {
        return port;
    }


    public NodeState getState() {
        return state;
    }

    public void setState(NodeState state) {
        this.state = state;
    }

    public NodeType getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    @Override
    public boolean equals(Object target) {
        if (target == null || !(target instanceof Node) || name == null || ((Node) target).name == null)
            return false;
        return name.equals(((Node) target).name);
    }

    @Override
    public String toString() {
        return String.format("name:%s", name);
    }

    @Override
    public int size() {
        return ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX
                + ByteUtils.SIZE_INT
                + ByteUtils.SIZE_INT
                + name.length()
                + addr.length()
                + ByteUtils.SIZE_SHORT
                + ByteUtils.SIZE_BYTE
                + ByteUtils.SIZE_BYTE;
    }

    @Override
    public byte[] getBytes() {
        int size = size();
        byte[] data = new byte[size];
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        pos = ByteUtils.putInt(data, pos, name.length());
        pos = ByteUtils.putInt(data, pos, addr.length());
        pos = ByteUtils.putString(data, pos, name);
        pos = ByteUtils.putString(data, pos, addr);
        pos = ByteUtils.putShort(data, pos, port);
        pos = ByteUtils.put(data, pos, state.getValue());
        ByteUtils.put(data, pos, type.getValue());
        return data;
    }
}
