package serde.plugin.gson;

import com.google.gson.Gson;
import com.lightgraph.graph.modules.serde.SerDe;

public class GsonSerde implements SerDe {

    @Override
    public <T> byte[] serialize(T object) {
        return new Gson().toJson(object).getBytes();
    }

    @Override
    public <T> byte[] serialize(T object, Class<T> clazz) {
        return new Gson().toJson(object).getBytes();
    }

    @Override
    public <T> T deSerialize(byte[] data, Class<T> clazz) {
        return new Gson().fromJson(new String(data), clazz);
    }

    @Override
    public <T> T deSerialize(byte[] data, int offset, int length, Class<T> clazz) {
        return new Gson().fromJson(new String(data, offset, length), clazz);
    }
}
