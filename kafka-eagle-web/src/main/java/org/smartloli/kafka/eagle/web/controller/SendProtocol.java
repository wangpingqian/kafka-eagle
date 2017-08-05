package org.smartloli.kafka.eagle.web.controller;
import org.smartloli.kafka.eagle.common.util.SystemConfigUtils;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.Node;
import java.nio.ByteBuffer;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.Integer;
/**
 * Created by user on 2017/7/17.
 */
public class SendProtocol {

private Logger LOG = LoggerFactory.getLogger(SendProtocol.class);

    Node node;
    public ByteBuffer send(String host, int port, AbstractRequest request, ApiKeys apiKey) throws IOException {
        Socket socket = connect(host, port);
        try {
            return send(request, apiKey, socket);
        } finally {
            socket.close();
        }

    }

    /**
     * 发送序列化请求并等待response返回
     *
     * @param socket  连向目标broker的socket
     * @param request 序列化后的请求
     * @return 序列化后的response
     * @throws IOException
     */
    private byte[] issueRequestAndWaitForResponse(Socket socket, byte[] request) throws IOException {
        sendRequest(socket, request);
        return getResponse(socket);
    }

    /**
     * 发送序列化请求给socket
     *
     * @param socket  连向目标broker的socket
     * @param request 序列化后的请求
     * @throws IOException
     */
    private void sendRequest(Socket socket, byte[] request) throws IOException {
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeInt(request.length);
        dos.write(request);
        dos.flush();
    }

    /**
     * 从给定socket处获取response
     *
     * @param socket 连向目标broker的socket
     * @return 获取到的序列化后的response
     * @throws IOException
     */
    private byte[] getResponse(Socket socket) throws IOException {
        DataInputStream dis = null;
        try {
            dis = new DataInputStream(socket.getInputStream());
            byte[] response = new byte[dis.readInt()];
            dis.readFully(response);
            return response;
        } finally {
            if (dis != null) {
                dis.close();
            }
        }
    }

    /**
     * 创建Socket连接
     *
     * @param hostName 目标broker主机名
     * @param port     目标broker服务端口, 比如9092
     * @return 创建的Socket连接
     * @throws IOException
     */
    private Socket connect(String hostName, int port) throws IOException {
        return new Socket(hostName, port);
    }

    /**
     * 向给定socket发送请求
     *
     * @param request 请求对象
     * @param apiKey  请求类型, 即属于哪种请求
     * @param socket  连向目标broker的socket
     * @return 序列化后的response
     * @throws IOException
     */
    private ByteBuffer send(AbstractRequest request, ApiKeys apiKey, Socket socket) throws IOException {
        RequestHeader header = new RequestHeader(apiKey.id, request.version(), "client-id", 0);
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + request.sizeOf());
        header.writeTo(buffer);
        request.writeTo(buffer);
        byte[] serializedRequest = buffer.array();
        byte[] response = issueRequestAndWaitForResponse(socket, serializedRequest);
        ByteBuffer responseBuffer = ByteBuffer.wrap(response);
        ResponseHeader.parse(responseBuffer);
        return responseBuffer;
    }




    public void  GroupCoordinator(String groupId) throws IOException {
        GroupCoordinatorRequest request = new GroupCoordinatorRequest.Builder(groupId).setVersion((short)0).build();
        String ip=SystemConfigUtils.getProperty("kafka.ip");
        String kafka_port=SystemConfigUtils.getProperty("kafka.port");
        int port=Integer.valueOf(kafka_port).intValue();
        ByteBuffer response = send(ip, port, request, ApiKeys.GROUP_COORDINATOR);
        GroupCoordinatorResponse resp = GroupCoordinatorResponse.parse(response);
        this.node=resp.node();
       //etc this.node=new Node(0,"118.89.197.56",9092);
        LOG.info("node1"+this.node.host());
        return ;
    }

    public MetadataResponse.TopicMetadata  Metadata(List<String> topics) throws IOException {
        LOG.info("node2"+this.node.host());
        MetadataRequest request=new MetadataRequest.Builder(topics).setVersion((short) 2).build();
        LOG.info("node3"+this.node.host());
        ByteBuffer response = send(this.node.host(), this.node.port(), request, ApiKeys.METADATA);
        LOG.info("node4"+this.node.host());
        MetadataResponse resp = MetadataResponse.parse(response);
        LOG.info("node5"+this.node.host());
        Collection<MetadataResponse.TopicMetadata> cresptm=resp.topicMetadata();
        LOG.info("node6"+this.node.host());
        Iterator it =cresptm.iterator();
        LOG.info("node7"+this.node.host());
        MetadataResponse.TopicMetadata resptm=(MetadataResponse.TopicMetadata)it.next();
        LOG.info("node8"+this.node.host());
       //var Iterator it2=resptm.partitionMetadata().iterator();
        return  resptm;

    }

    public Long listoffsets(TopicPartition tp, String host, int port) throws IOException {
        LOG.info("offset1 "+host);
        Map mtp = new HashMap();
        LOG.info("offset2 "+port);
        mtp.put(tp, ListOffsetRequest.EARLIEST_TIMESTAMP);
        LOG.info("offset3 ");
        ListOffsetRequest request = new ListOffsetRequest.Builder(1).setTargetTimes(mtp).setVersion((short) 1).build();
        LOG.info("offset4 ");
        ByteBuffer response = send(host, port, request, ApiKeys.LIST_OFFSETS);
        LOG.info("offset5 ");
        ListOffsetResponse resp = ListOffsetResponse.parse(response);
        LOG.info("offset6 ");
        Map map1 = resp.responseData();
        LOG.info("offset7 ");
        Iterator entries = map1.entrySet().iterator();
        LOG.info("offset8 ");
        Map.Entry entry = (Map.Entry) entries.next();
        LOG.info("offset9 ");
        TopicPartition key = (TopicPartition) entry.getKey();
        LOG.info("offset10 ");
        ListOffsetResponse.PartitionData value = (ListOffsetResponse.PartitionData) entry.getValue();
        LOG.info("offset11 "+value.offset);
        return value.offset;

    }
}
