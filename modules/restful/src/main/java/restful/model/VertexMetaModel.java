package restful.model;

import com.lightgraph.graph.meta.DataType;

import java.util.List;

public class VertexMetaModel {

    private String key;
    private List<PropertyMetaModel> properties;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<PropertyMetaModel> getProperties() {
        return properties;
    }

    public void setProperties(List<PropertyMetaModel> properties) {
        this.properties = properties;
    }

    public static class PropertyMetaModel {

        private String name;
        private DataType dataType;
        private Object defaultValue;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public DataType getDataType() {
            return dataType;
        }

        public void setDataType(DataType dataType) {
            this.dataType = dataType;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
        }
    }
}
