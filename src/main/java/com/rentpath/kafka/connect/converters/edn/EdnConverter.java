package com.rentpath.kafka.connect.converters.edn;

import com.google.common.base.CaseFormat;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import us.bpsm.edn.Keyword;
import us.bpsm.edn.Named;
import us.bpsm.edn.Symbol;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser;
import us.bpsm.edn.parser.Parsers;
import us.bpsm.edn.printer.Printers;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

public class EdnConverter implements Converter {
    private Cache<Map<Keyword, Object>, Schema> toConnectSchemaCache;
    private Cache<Schema, Map<Keyword, Object>> fromConnectSchemaCache;
    private Parser.Config parserConfig;
    private Parser parser;
    private EdnConverterConfig config;

    private static final Map<Schema.Type, EdnToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap<>(Schema.Type.class);
    private static final String SCHEMA_KEYWORD_NAME = "_schema";
    private static final String SCHEMA_VERSION_KEYWORD_NAME = "version";
    private static final String SCHEMA_TYPE_KEYWORD_NAME = "type";
    private static final Keyword SCHEMA_VERSION_KEYWORD = Keyword.newKeyword(SCHEMA_KEYWORD_NAME, SCHEMA_VERSION_KEYWORD_NAME);
    private static final Keyword SCHEMA_TYPE_KEYWORD = Keyword.newKeyword(SCHEMA_KEYWORD_NAME, SCHEMA_TYPE_KEYWORD_NAME);

    static {
        TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                return (boolean)value;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                return (byte)value;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                return (short)value;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                return (int)value;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                if (value instanceof java.util.Date)
                    return ((java.util.Date)value).getTime();
                if (value instanceof GregorianCalendar)
                    return ((GregorianCalendar)value).getTimeInMillis();
                else
                    return (long)value;

            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                return (float)value;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                return (double)value;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                Keyword kw;
                Symbol sym;
                String delim = "-";
                String rawKeystr;
                if (value instanceof UUID)
                    return ((UUID)value).toString();
                else if (value instanceof Named) {
                    kw = (Keyword)value;
                    if (kw.getPrefix() != null && !kw.getPrefix().equals("")) {
                        if (config.internalKeyFormat().equals(CaseFormat.LOWER_UNDERSCORE))
                            delim = "_";
                        else if (config.internalKeyFormat().equals(CaseFormat.UPPER_CAMEL))
                            delim = "";
                        rawKeystr = kw.getPrefix() + delim + kw.getName();
                    } else
                        rawKeystr = kw.getName();
                    return config.internalKeyFormat().to(config.externalKeyFormat(), rawKeystr);
                } else
                    return (String)value;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                if (value instanceof BigInteger)
                    return ((BigInteger)value);
                if (value instanceof BigDecimal)
                    return ((BigDecimal)value);
                else
                    return (byte[])value;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                Schema elemSchema = schema.valueSchema();
                List<Object> result = new ArrayList<>();
                for (Object elem : (Collection)value) {
                    result.add(convertToConnect(config, elemSchema, elem));
                }
                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                Schema keySchema = schema.keySchema();
                Schema valueSchema = schema.valueSchema();

                // Trust the schema extraction - if it says it's a Map (as opposed to a Struct), we need to assume that
                // that's in fact the case.
                Map<Object, Object> result = new HashMap<>();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>)value).entrySet()) {
                    result.put(
                        convertToConnect(config, keySchema, entry.getKey()),
                        convertToConnect(config, valueSchema, entry.getValue())
                    );
                }
                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                Map<Keyword, ?> map = (Map<Keyword, ?>)value;
                Struct result = new Struct(schema.schema());
                for (Field field : schema.fields()) {
                    KeywordField kwField = (KeywordField)field;
                    result.put(field, convertToConnect(config, field.schema(), map.get(kwField.keyword())));
                }
                return result;
            }
        });
    }

    private static final HashMap<String, LogicalTypeConverter> TO_CONNECT_LOGICAL_CONVERTERS = new HashMap<>();

    static {
        TO_CONNECT_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (value instanceof BigDecimal)
                    return (BigDecimal)value;
                if (value instanceof BigInteger)
                    return new BigDecimal((BigInteger)value, 0);
                else
                    throw new DataException("Invalid type for Decimal, underlying representation should be BigDecimal but was " + value.getClass());
            }
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof Integer))
                    throw new DataException("Invalid type for Date, underlying representation should be int32 but was " + value.getClass());
                return Date.toLogical(schema, (int) value);
            }
        });
    }

    public static Schema valueToSchema(Object value) {
        return null;
    }

    public EdnConverter() {}

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        config = new EdnConverterConfig(map);
        parser = Parsers.newParser(Parsers.defaultConfiguration());
        toConnectSchemaCache = new SynchronizedCache<>(new LRUCache<Map<Keyword, Object>, Schema>(config.schemaCacheSize()));
        fromConnectSchemaCache = new SynchronizedCache<>(new LRUCache<Schema, Map<Keyword, Object>>(config.schemaCacheSize()));
    }

    private interface LogicalTypeConverter {
        Object convert(Schema schema, Object value);
    }

    private interface EdnToConnectTypeConverter {
        Object convert(EdnConverterConfig config, Schema schema, Object value);
    }

    private Map<Keyword, Object> convertToEdnWithEnvelope(EdnConverterConfig config, Schema schema, Object value) {
        Map<Keyword, Object> map = new HashMap<>();
        map.put(EdnSchema.SCHEMA_FIELD, asEdnSchema(schema));
        map.put(EdnSchema.PAYLOAD_FIELD, convertToEdn(config, schema, value));
        return map;
    }

    private static Object convertToEdn(EdnConverterConfig config, Schema schema, Object value) {
        if (value == null) {
            if (schema == null || schema.isOptional()) // Any schema is valid and we don't have a default, so treat this as an optional schema
                return null;
            if (schema.defaultValue() != null)
                return convertToEdn(config, schema, schema.defaultValue());
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        try {
            final Schema.Type schemaType;
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(value.getClass());
                if (schemaType == null)
                    throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
            } else {
                schemaType = schema.type();
            }
            switch (schemaType) {
                case INT8:
                    return (byte)value;
                case INT16:
                    return (short)value;
                case INT32:
                    return (int)value;
                case INT64:
                    return (long)value;
                case FLOAT32:
                    return (float)value;
                case FLOAT64:
                    return (double)value;
                case BOOLEAN:
                    return (boolean)value;
                case STRING:
                    return (String)value;
                case BYTES:
                    if (value instanceof byte[])
                        return (byte[]) value;
                    else if (value instanceof ByteBuffer)
                        return ((ByteBuffer) value).array();
                    else
                        throw new DataException("Invalid type for bytes type: " + value.getClass());
                case ARRAY: {
                    Collection collection = (Collection) value;
                    List<Object> list = new ArrayList<Object>();
                    for (Object elem : collection) {
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        Object ednValue = convertToEdn(config, valueSchema, elem);
                        list.add(ednValue);
                    }
                    return list;
                }
                case MAP: {
                    Map<?, ?> map = (Map<?, ?>) value;
                    Map<Object, Object> convertedMap = new HashMap<Object, Object>();
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        Schema keySchema = schema.keySchema();
                        Schema valueSchema = schema.valueSchema();
                        Object newKey = convertToEdn(config, keySchema, entry.getKey());
                        Object newValue = convertToEdn(config, valueSchema, entry.getValue());
                        convertedMap.put(newKey, newValue);
                    }
                    return convertedMap;
                }
                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (!struct.schema().equals(schema))
                        throw new DataException("Mismatching schema.");
                    Map<Keyword, Object> convertedStruct = new HashMap<Keyword, Object>();
                    for (Field field : schema.fields()) {
                        Object newValue = convertToEdn(config, schema, struct.get(field));
                        CaseFormat externalFormat = config.externalKeyFormat();
                        CaseFormat internalFormat = config.internalKeyFormat();
                        String[] nameParts = field.name().split("/");
                        String convertedKeyNamespace, convertedKeyName;
                        Keyword newKey;
                        if (nameParts.length > 1) {
                            convertedKeyNamespace = externalFormat.to(internalFormat, nameParts[0]);
                            convertedKeyName = externalFormat.to(internalFormat, nameParts[1]);
                            newKey = Keyword.newKeyword(convertedKeyNamespace, convertedKeyName);
                        } else {
                            convertedKeyName = externalFormat.to(internalFormat, field.name());
                            newKey = Keyword.newKeyword(convertedKeyName);
                        }
                        convertedStruct.put(newKey, newValue);
                    }
                    return convertedStruct;
                }
            }
            if (value instanceof BigDecimal)
                return (BigDecimal)value;
            if (value instanceof java.util.Date)
                return ((java.util.Date)value);

            throw new DataException("Couldn't convert " + value + " to EDN.");
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
        }
    }

    private static Object convertToConnect(EdnConverterConfig config, Schema schema, Object value) {
        final Schema.Type schemaType = schema.type();

        if (value == null) {
            if (schema.defaultValue() != null)
                return schema.defaultValue(); // any logical type conversions should already have been applied
            if (schema.isOptional())
                return null;
            throw new DataException("Invalid null value for required " + schemaType +  " field");
        }

        final EdnToConnectTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
        if (typeConverter == null)
            throw new DataException("Unknown schema type: " + String.valueOf(schemaType));

        Object converted = typeConverter.convert(config, schema, value);
        if (schema.name() != null) {
            LogicalTypeConverter logicalConverter = TO_CONNECT_LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null)
                converted = logicalConverter.convert(schema, converted);
        }
        return converted;
    }

    @Override
    public byte[] fromConnectData(String s, Schema schema, Object o) {
        return Printers.printString(Printers.defaultPrinterProtocol(), convertToEdnWithEnvelope(config, schema, o)).getBytes();
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] bytes) {
        if (bytes == null)
            return SchemaAndValue.NULL;
        Parseable p = Parsers.newParseable(new String(bytes));
        Object parsed = parser.nextValue(p);
        if (!(parsed instanceof Map))
            throw new DataException("Root message EDN node not a map. Message invalid for sinking.");
        Map<Keyword, ?> rootMap = (Map<Keyword, ?>)parsed;
        if (!rootMap.containsKey(EdnSchema.SCHEMA_FIELD))
            throw new DataException("Root message EDN node has no schema field. Schema required for sinking.");
        if (!(rootMap.get(EdnSchema.PAYLOAD_FIELD) instanceof Map))
            throw new DataException("Root message EDN node schema field malformed. Valid schema required for sinking.");
        if (!rootMap.containsKey(EdnSchema.PAYLOAD_FIELD))
            throw new DataException("Root message EDN node has no payload field. Payload required for sinking.");
        Schema schema = asConnectSchema((Map<Keyword, Object>)rootMap.get(EdnSchema.SCHEMA_FIELD));
        return new SchemaAndValue(schema, convertToConnect(config, schema, parsed));
    }

    private Map<Keyword, Object> asEdnSchema(Schema schema) {
        if (schema == null)
            return null;
        Map<Keyword, Object> cached = fromConnectSchemaCache.get(schema);
        if (cached != null)
            return cached;
        final Map<Keyword, Object> ednSchema;
        switch (schema.type()) {
            case BOOLEAN:
                ednSchema = EdnSchema.BOOLEAN_SCHEMA.asMap();
                break;
            case INT8:
                ednSchema = EdnSchema.INT8_SCHEMA.asMap();
                break;
            case INT16:
                ednSchema = EdnSchema.INT16_SCHEMA.asMap();
                break;
            case INT32:
                ednSchema = EdnSchema.INT32_SCHEMA.asMap();
                break;
            case INT64:
                ednSchema = EdnSchema.INT64_SCHEMA.asMap();
                break;
            case FLOAT32:
                ednSchema = EdnSchema.FLOAT32_SCHEMA.asMap();
                break;
            case FLOAT64:
                ednSchema = EdnSchema.FLOAT64_SCHEMA.asMap();
                break;
            case STRING:
                ednSchema = EdnSchema.STRING_SCHEMA.asMap();
                break;
            case ARRAY:
                ednSchema = new HashMap<>();
                ednSchema.put(EdnSchema.SCHEMA_TYPE_FIELD, EdnSchema.ARRAY_TYPE_NAME);
                ednSchema.put(EdnSchema.ARRAY_ITEMS_FIELD, asEdnSchema(schema.valueSchema()));
                break;
            case MAP:
                ednSchema = new HashMap<>();
                ednSchema.put(EdnSchema.SCHEMA_TYPE_FIELD, EdnSchema.MAP_TYPE_NAME);
                ednSchema.put(EdnSchema.MAP_KEY_FIELD, asEdnSchema(schema.keySchema()));
                ednSchema.put(EdnSchema.MAP_VALUE_FIELD, asEdnSchema(schema.valueSchema()));
                break;
            case STRUCT:
                ednSchema = new HashMap<>();
                ednSchema.put(EdnSchema.SCHEMA_TYPE_FIELD, EdnSchema.STRUCT_TYPE_NAME);
                List<Map<Keyword, Object>> fields = new ArrayList<>();
                for (Field field : schema.fields()) {
                    Map<Keyword, Object> fieldEdnSchema = asEdnSchema(field.schema());
                    fieldEdnSchema.put(EdnSchema.STRUCT_FIELD_NAME_FIELD, field.name());
                    fieldEdnSchema.put(EdnSchema.STRUCT_FIELD_KEYWORD_FIELD, stringToKeyword(config, field.name()));
                    fields.add(fieldEdnSchema);
                }
                ednSchema.put(EdnSchema.STRUCT_FIELDS_FIELD, fields);
                break;
            default:
                throw new DataException("Couldn't translate unsupported schema type " + schema + ".");
        }
        ednSchema.put(EdnSchema.SCHEMA_OPTIONAL_FIELD, schema.isOptional());
        if (schema.name() != null)
            ednSchema.put(EdnSchema.SCHEMA_NAME_FIELD, schema.name());
        if (schema.version() != null)
            ednSchema.put(EdnSchema.SCHEMA_VERSION_FIELD, schema.version());
        if (schema.doc() != null)
            ednSchema.put(EdnSchema.SCHEMA_DOC_FIELD, schema.doc());
        if (schema.parameters() != null) {
            Map<Keyword, Object> ednSchemaParams = new HashMap<>();
            for (Map.Entry<String, String> prop : schema.parameters().entrySet())
                ednSchemaParams.put(Keyword.newKeyword(prop.getKey()), prop.getValue());
            ednSchema.put(EdnSchema.SCHEMA_PARAMETERS_FIELD, ednSchemaParams);
        }
        if (schema.defaultValue() != null)
            ednSchema.put(EdnSchema.SCHEMA_DEFAULT_FIELD, convertToEdn(config, schema, schema.defaultValue()));
        fromConnectSchemaCache.put(schema, ednSchema);
        return ednSchema;
    }

    private Schema asConnectSchema(Map<Keyword, Object> ednSchema) {
        if (ednSchema == null)
            return null;
        Schema cached = toConnectSchemaCache.get(ednSchema);
        if (cached != null)
            return cached;
        String type = (String)ednSchema.get(EdnSchema.SCHEMA_TYPE_FIELD);
        if (type == null)
            throw new DataException("Schema must contain :type field.");
        final SchemaBuilder builder;
        switch (type) {
            case EdnSchema.BOOLEAN_TYPE_NAME:
                builder = SchemaBuilder.bool();
                break;
            case EdnSchema.INT8_TYPE_NAME:
                builder = SchemaBuilder.int8();
                break;
            case EdnSchema.INT16_TYPE_NAME:
                builder = SchemaBuilder.int16();
                break;
            case EdnSchema.INT32_TYPE_NAME:
                builder = SchemaBuilder.int32();
                break;
            case EdnSchema.INT64_TYPE_NAME:
                builder = SchemaBuilder.int64();
                break;
            case EdnSchema.FLOAT32_TYPE_NAME:
                builder = SchemaBuilder.float32();
                break;
            case EdnSchema.FLOAT64_TYPE_NAME:
                builder = SchemaBuilder.float64();
                break;
            case EdnSchema.BYTES_TYPE_NAME:
                builder = SchemaBuilder.bytes();
                break;
            case EdnSchema.STRING_TYPE_NAME:
                builder = SchemaBuilder.string();
                break;
            case EdnSchema.ARRAY_TYPE_NAME:
                Map<Keyword, Object> elemSchema = (Map<Keyword, Object>)ednSchema.get(EdnSchema.ARRAY_ITEMS_FIELD);
                if (elemSchema == null)
                    throw new DataException("Array schema did not specify element type.");
                builder = SchemaBuilder.array(asConnectSchema(elemSchema));
                break;
            case EdnSchema.MAP_TYPE_NAME:
                Map<Keyword, Object> keySchema = (Map<Keyword, Object>)ednSchema.get(EdnSchema.MAP_KEY_FIELD);
                if (keySchema == null)
                    throw new DataException("Map schema did not specify key type.");
                Map<Keyword, Object> valueSchema = (Map<Keyword, Object>)ednSchema.get(EdnSchema.MAP_VALUE_FIELD);
                if (valueSchema == null)
                    throw new DataException("Map schema did not specify value type.");
                builder = SchemaBuilder.map(asConnectSchema(keySchema), asConnectSchema(valueSchema));
                break;
            case EdnSchema.STRUCT_TYPE_NAME:
                builder = KeywordFieldSchemaBuilder.struct();
                List<Map<Keyword, Object>> fields = (List<Map<Keyword, Object>>)ednSchema.get(EdnSchema.STRUCT_FIELDS_FIELD);
                if (fields == null)
                    throw new DataException("Struct schema did not specify field names, keywords, and types.");
                for (Map<Keyword, ?> field : fields) {
                    Keyword keyword = (Keyword)field.get(EdnSchema.STRUCT_FIELD_KEYWORD_FIELD);
                    if (keyword == null)
                        throw new DataException("Struct field schema did not specify field keyword.");
                    String name = (String)field.get(EdnSchema.STRUCT_FIELD_NAME_FIELD);
                    if (keyword == null)
                        throw new DataException("Struct field schema did not specify field name.");
                    ((KeywordFieldSchemaBuilder)builder).keywordField(keyword, name, asConnectSchema((Map<Keyword, Object>)field));
                }
                break;
            default:
                throw new DataException("Unknown schema type: " + type);
        }

        if (ednSchema.containsKey(EdnSchema.SCHEMA_OPTIONAL_FIELD)) {
            Boolean schemaOptional = (Boolean) ednSchema.get(EdnSchema.SCHEMA_OPTIONAL_FIELD);
            if (schemaOptional != null && schemaOptional)
                builder.optional();
            else
                builder.required();
        }

        if (ednSchema.containsKey(EdnSchema.SCHEMA_NAME_FIELD)) {
            String schemaName = (String)ednSchema.get(EdnSchema.SCHEMA_NAME_FIELD);
            if (schemaName != null)
                builder.name(schemaName);
        }

        if (ednSchema.containsKey(EdnSchema.SCHEMA_VERSION_FIELD)) {
            Integer schemaVersion = (Integer) ednSchema.get(EdnSchema.SCHEMA_VERSION_FIELD);
            if (schemaVersion != null)
                builder.version(schemaVersion);
        }

        if (ednSchema.containsKey(EdnSchema.SCHEMA_DOC_FIELD)) {
            String schemaDoc = (String) ednSchema.get(EdnSchema.SCHEMA_DOC_FIELD);
            if (schemaDoc != null)
                builder.doc(schemaDoc);
        }

        if (ednSchema.containsKey(EdnSchema.SCHEMA_PARAMETERS_FIELD)) {
            Map<Keyword, String> schemaParams = (Map<Keyword, String>) ednSchema.get(EdnSchema.SCHEMA_PARAMETERS_FIELD);
            for (Map.Entry<Keyword, String> entry : schemaParams.entrySet()) {
                builder.parameter(keywordToString(config, entry.getKey()), entry.getValue());
            }
        }

        if (ednSchema.containsKey(EdnSchema.SCHEMA_DEFAULT_FIELD)) {
            Map<Keyword, ?> schemaDefault = (Map<Keyword, ?>)ednSchema.get(EdnSchema.SCHEMA_DEFAULT_FIELD);
            if (schemaDefault != null)
                builder.defaultValue(convertToConnect(config, builder.schema(), schemaDefault));
        }

        Schema result = builder.build();
        toConnectSchemaCache.put(ednSchema, result);
        return result;
    }

    private static String keywordToString(EdnConverterConfig config, Keyword kw) {
        String delim =  "";
        String rawKeyStr = kw.getName();
        if (kw.getPrefix() != null && !kw.getPrefix().equals("")) {
            if (config.internalKeyFormat().equals(CaseFormat.LOWER_UNDERSCORE))
                delim = "_";
            else if (config.internalKeyFormat().equals(CaseFormat.UPPER_CAMEL))
                delim = "";
            rawKeyStr = kw.getPrefix() + delim + kw.getName();
        }
        return config.internalKeyFormat().to(config.externalKeyFormat(), rawKeyStr);
    }

    private static Keyword stringToKeyword(EdnConverterConfig config, String string) {
        CaseFormat externalFormat = config.externalKeyFormat();
        CaseFormat internalFormat = config.internalKeyFormat();
        String[] nameParts = string.split("/");
        String convertedKeyNamespace, convertedKeyName;
        Keyword newKey;
        if (nameParts.length > 1) {
            convertedKeyNamespace = externalFormat.to(internalFormat, nameParts[0]);
            convertedKeyName = externalFormat.to(internalFormat, nameParts[1]);
            newKey = Keyword.newKeyword(convertedKeyNamespace, convertedKeyName);
        } else {
            convertedKeyName = externalFormat.to(internalFormat, string);
            newKey = Keyword.newKeyword(convertedKeyName);
        }
        return newKey;
    }
}
