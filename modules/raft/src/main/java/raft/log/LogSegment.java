package raft.log;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.utils.ByteUtils;

import java.io.File;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static com.lightgraph.graph.constant.GraphConstant.DIR_SEPARATOR;

public class LogSegment {
    public static final String DATA_SUFFIX = "dat";
    public static final String INDEX_SUFFIX = "idx";
    public static final String FILE_PATTERN = "%s" + GraphConstant.FILE_SUFFIX_TOKEN + "%s";
    private final String dataPath;
    private final String indexPath;
    private FileChannel dataChannel;
    private FileChannel indexChannel;
    private long initIndex;
    private long startIndex;
    private long endIndex;

    public LogSegment(String logPath, long index) {
        this.dataPath = logPath + DIR_SEPARATOR + String.format(FILE_PATTERN, index, DATA_SUFFIX);
        this.indexPath = logPath + DIR_SEPARATOR + String.format(FILE_PATTERN, index, INDEX_SUFFIX);
        initIndex = index;
    }

    public void open() {
        try {
            dataChannel = FileChannel.open(new File(dataPath).toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            indexChannel = FileChannel.open(new File(indexPath).toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            ByteBuffer buffer = ShortSafeBuffer.getTLB();
            if (indexChannel.read(buffer, 0) > 0) {
                buffer.flip();
                startIndex = buffer.getLong();
            } else {
                startIndex = initIndex;
            }
            buffer.clear();
            if (indexChannel.size() >= 16 && indexChannel.read(buffer, indexChannel.size() - 16) > 0) {
                buffer.flip();
                endIndex = buffer.getLong();
            } else {
                endIndex = startIndex;
            }
        } catch (IOException e) {
            throw new GraphException(String.format("open (%s,%s) failed!", dataPath, indexPath));
        }
    }

    public void close() throws IOException {
        dataChannel.close();
        indexChannel.close();
    }

    public LogEdit seekTo(long index) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        long position = (index - startIndex) * 16 + 8;
        indexChannel.read(buffer, position);
        buffer.flip();
        long dataPos = buffer.getLong();
        buffer.clear();
        dataChannel.read(buffer, dataPos);
        buffer.flip();
        long preTerm = ByteUtils.readVarlong(buffer);
        long term = ByteUtils.readVarlong(buffer);
        long curIndex = ByteUtils.readVarlong(buffer);
        int dataLength = ByteUtils.readVarint(buffer);
        byte[] data = new byte[dataLength];
        ByteBuffer dataBuffer = ByteBuffer.allocate(dataLength);
        dataChannel.read(dataBuffer, dataPos + buffer.position());
        dataBuffer.flip();
        dataBuffer.get(data);
        return new LogEdit(data, preTerm, term, curIndex);
    }

    public void truncateTo(long index) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(24);
        long position = (index - startIndex + 1) * 16 + 8;
        indexChannel.read(buffer, position);
        buffer.flip();
        long dataPos = buffer.getLong();
        indexChannel.truncate(position - 8);
        dataChannel.truncate(dataPos);
        endIndex = index;
    }

    public void append(LogEdit edit) {
        ByteBuffer buffer = edit.toByteBuffer();
        int size = buffer.limit();
        try {
            if (startIndex == -1)
                startIndex = edit.getIndex();
            dataChannel.write(buffer);
            dataChannel.force(true);
            buffer = ShortSafeBuffer.getTLB();
            buffer.putLong(edit.getIndex());
            buffer.putLong(dataChannel.position() - size);
            buffer.flip();
            indexChannel.write(buffer);
            indexChannel.force(true);
            endIndex++;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getDataSize() throws IOException {
        return dataChannel.size();
    }

    public long getStartIndex() {
        return startIndex;
    }

    public long getEndIndex() {
        return endIndex;
    }

    public boolean delete() {
        File file = new File(indexPath);
        boolean deleted = false;
        if (file.exists()) {
            deleted = file.delete();
        }
        file = new File(dataPath);
        if (file.exists()) {
            deleted = deleted && file.delete();
        }
        return deleted;
    }
}