package com.rentpath.kafka.connect.converters.edn;

import us.bpsm.edn.Keyword;

import java.util.HashMap;
import java.util.Map;

public class EdnSchema {
    static final Keyword SCHEMA_FIELD = Keyword.newKeyword("schema");
    static final Keyword PAYLOAD_FIELD = Keyword.newKeyword("payload");
    static final Keyword SCHEMA_TYPE_FIELD = Keyword.newKeyword("type");
    static final Keyword SCHEMA_OPTIONAL_FIELD = Keyword.newKeyword("optional");
    static final Keyword SCHEMA_NAME_FIELD = Keyword.newKeyword("name");
    static final Keyword SCHEMA_VERSION_FIELD = Keyword.newKeyword("version");
    static final Keyword SCHEMA_DOC_FIELD = Keyword.newKeyword("doc");
    static final Keyword SCHEMA_PARAMETERS_FIELD = Keyword.newKeyword("parameters");
    static final Keyword SCHEMA_DEFAULT_FIELD = Keyword.newKeyword("default");
    static final Keyword ARRAY_ITEMS_FIELD = Keyword.newKeyword("items");
    static final Keyword MAP_KEY_FIELD = Keyword.newKeyword("keys");
    static final Keyword MAP_VALUE_FIELD = Keyword.newKeyword("values");
    static final Keyword STRUCT_FIELDS_FIELD = Keyword.newKeyword("fields");
    static final Keyword STRUCT_FIELD_NAME_FIELD = Keyword.newKeyword("field");
    static final Keyword STRUCT_FIELD_KEYWORD_FIELD = Keyword.newKeyword("keyword");

    static final String BOOLEAN_TYPE_NAME = "boolean";
    static final EdnSchema BOOLEAN_SCHEMA = new EdnSchema(SCHEMA_TYPE_FIELD, BOOLEAN_TYPE_NAME);
    static final String INT8_TYPE_NAME = "int8";
    static final EdnSchema INT8_SCHEMA = new EdnSchema(SCHEMA_TYPE_FIELD, INT8_TYPE_NAME);
    static final String INT16_TYPE_NAME = "int16";
    static final EdnSchema INT16_SCHEMA = new EdnSchema(SCHEMA_TYPE_FIELD, INT16_TYPE_NAME);
    static final String INT32_TYPE_NAME = "int32";
    static final EdnSchema INT32_SCHEMA = new EdnSchema(SCHEMA_TYPE_FIELD, INT32_TYPE_NAME);
    static final String INT64_TYPE_NAME = "int64";
    static final EdnSchema INT64_SCHEMA = new EdnSchema(SCHEMA_TYPE_FIELD, INT64_TYPE_NAME);
    static final String FLOAT32_TYPE_NAME = "float";
    static final EdnSchema FLOAT32_SCHEMA = new EdnSchema(SCHEMA_TYPE_FIELD, FLOAT32_TYPE_NAME);
    static final String FLOAT64_TYPE_NAME = "double";
    static final EdnSchema FLOAT64_SCHEMA = new EdnSchema(SCHEMA_TYPE_FIELD, FLOAT64_TYPE_NAME);
    static final String BYTES_TYPE_NAME = "bytes";
    static final EdnSchema BYTES_SCHEMA = new EdnSchema(SCHEMA_TYPE_FIELD, BYTES_TYPE_NAME);
    static final String STRING_TYPE_NAME = "string";
    static final EdnSchema STRING_SCHEMA = new EdnSchema(SCHEMA_TYPE_FIELD, STRING_TYPE_NAME);
    static final String ARRAY_TYPE_NAME = "array";
    static final String MAP_TYPE_NAME = "map";
    static final String STRUCT_TYPE_NAME = "struct";

    private Keyword key;
    private Object value;

    public EdnSchema(Keyword key, Object value) {
        this.key = key;
        this.value = value;
    }

    public Map<Keyword, Object> asMap() {
        Map<Keyword, Object> map = new HashMap<>();
        map.put(this.key, this.value);
        return map;
    }
}
