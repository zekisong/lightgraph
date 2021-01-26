package serde.plugin.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.lightgraph.graph.modules.serde.SerDe;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class KryoSerde implements SerDe {

    private ThreadLocal<Kryo> kryo = new ThreadLocal<>();

    @Override
    public <T> byte[] serialize(T object) {
        Kryo instance = getInstance();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        instance.writeObject(output, object);
        return output.toBytes();
    }

    @Override
    public <T> byte[] serialize(T object, Class<T> clazz) {
        Kryo instance = getInstance();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        instance.writeObjectOrNull(output, object, clazz);
        return output.toBytes();
    }

    @Override
    public <T> T deSerialize(byte[] data, Class<T> clazz) {
        Kryo instance = getInstance();
        return instance.readObject(new Input(data), clazz);
    }

    @Override
    public <T> T deSerialize(byte[] data, int offset, int length, Class<T> clazz) {
        Kryo instance = getInstance();
        return instance.readObject(new Input(data, offset, length), clazz);
    }

    public Kryo getInstance() {
        Kryo instance = kryo.get();
        if (instance == null) {
            instance = new Kryo();
            kryo.set(instance);
        }
        return instance;
    }
}
