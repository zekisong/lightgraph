package raft.log;

import com.lightgraph.graph.utils.ByteUtils;

import java.nio.ByteBuffer;

public class LogEdit {

    private long term;
    private long preTerm;
    private long index;
    private int dataLength;
    private byte[] data;

    public LogEdit(byte[] data, long preTerm, long term, long index) {
        this.data = data;
        this.preTerm = preTerm;
        this.term = term;
        this.index = index;
        this.dataLength = data.length;
    }

    public long getTerm() {
        return term;
    }

    public long getPreTerm() {
        return preTerm;
    }

    public long getIndex() {
        return index;
    }

    public int getDataLength() {
        return dataLength;
    }

    public byte[] getData() {
        return data;
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ShortSafeBuffer.getTLB();
        ByteUtils.writeVarlong(preTerm, buffer);
        ByteUtils.writeVarlong(term, buffer);
        ByteUtils.writeVarlong(index, buffer);
        ByteUtils.writeVarint(dataLength, buffer);
        buffer.put(data);
        buffer.flip();
        return buffer;
    }

    @Override
    public String toString() {
        return String.format("term:%d,index:%d:len:%d,data:%s", term, index, dataLength, new String(data));
    }
}
