package com.rentpath.kafka.connect.converters;

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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

public class EdnConverter implements Converter {
    private Cache<Object, Schema> toConnectSchemaCache;
    private Parser.Config parserConfig;
    private Parser parser;
    private EdnConverterConfig config;

    private static final Map<Schema.Type, EdnToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap<>(Schema.Type.class);

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
                Schema elemSchema = schema == null ? null : schema.valueSchema();
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
                Schema keySchema = schema == null ? null : schema.keySchema();
                Schema valueSchema = schema == null ? null : schema.valueSchema();

                // If the map uses strings for keys, it should be encoded in the natural JSON format. If it uses other
                // primitive types or a complex type as a key, it will be encoded as a list of pairs. If we don't have a
                // schema, we default to encoding in a Map.
                Map<Object, Object> result = new HashMap<>();
                if (schema == null || keySchema.type() == Schema.Type.STRING) {
                    if (!value.isObject())
                        throw new DataException("Maps with string fields should be encoded as JSON objects, but found " + value.getNodeType());
                    Iterator<Map.Entry<String, Object>> fieldIt = value.fields();
                    while (fieldIt.hasNext()) {
                        Map.Entry<String, Object> entry = fieldIt.next();
                        result.put(entry.getKey(), convertToConnect(config, valueSchema, entry.getValue()));
                    }
                } else {
                    if (!value.isArray())
                        throw new DataException("Maps with non-string fields should be encoded as JSON array of tuples, but found " + value.getNodeType());
                    for (Object entry : value) {
                        if (!entry.isArray())
                            throw new DataException("Found invalid map entry instead of array tuple: " + entry.getNodeType());
                        if (entry.size() != 2)
                            throw new DataException("Found invalid map entry, expected length 2 but found :" + entry.size());
                        result.put(convertToConnect(config, keySchema, entry.get(0)),
                                convertToConnect(config, valueSchema, entry.get(1)));
                    }
                }
                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, new EdnToConnectTypeConverter() {
            @Override
            public Object convert(EdnConverterConfig config, Schema schema, Object value) {
                if (!value.isObject())
                    throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());

                // We only have ISchema here but need Schema, so we need to materialize the actual schema. Using ISchema
                // avoids having to materialize the schema for non-Struct types but it cannot be avoided for Structs since
                // they require a schema to be provided at construction. However, the schema is only a SchemaBuilder during
                // translation of schemas to JSON; during the more common translation of data to JSON, the call to schema.schema()
                // just returns the schema Object and has no overhead.
                Struct result = new Struct(schema.schema());
                for (Field field : schema.fields())
                    result.put(field, convertToConnect(config, field.schema(), value.get(field.name())));

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
        toConnectSchemaCache = new SynchronizedCache<>(new LRUCache<Object, Schema>(config.schemaCacheSize()));
    }

    private interface LogicalTypeConverter {
        Object convert(Schema schema, Object value);
    }

    private interface EdnToConnectTypeConverter {
        Object convert(EdnConverterConfig config, Schema schema, Object value);
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
                        Schema keySchema = schema == null ? null : schema.keySchema();
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        Object newKey = convertToEdn(config, keySchema, entry.getKey());
                        Object newValue = convertToEdn(config, valueSchema, entry.getValue());
                        if (newKey instanceof String) {
                            CaseFormat externalFormat = config.externalKeyFormat();
                            CaseFormat internalFormat = config.internalKeyFormat();
                            String convertedKeyName = externalFormat.to(internalFormat, (String)newKey);
                            if (config.keywordizeKeys())
                                newKey = Keyword.newKeyword(convertedKeyName);
                            else
                                newKey = convertedKeyName;
                        }
                        convertedMap.put(newKey, newValue);
                    }
                    return convertedMap;
                }
                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (!struct.schema().equals(schema))
                        throw new DataException("Mismatching schema.");
                    Map<Object, Object> convertedStruct = new HashMap<Object, Object>();
                    for (Field field : schema.fields()) {
                        convertedStruct.put(field.name(), convertToEdn(config, field.schema(), struct.get(field)));
                    }
                    return null;
                }
            }
            if (value instanceof BigDecimal)
                return (BigDecimal)value;
            if (value instanceof Date)
                return (Date)value;

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
        return Printers.printString(Printers.defaultPrinterProtocol(), convertToEdn(config, schema, o)).getBytes();
    }

    @Override
    public SchemaAndValue toConnectData(String s, byte[] bytes) {
        Parseable p = Parsers.newParseable(new String(bytes));
        Map<?, ?> m = (Map<?, ?>)parser.nextValue(p);
    }

    private Schema asConnectSchema(Object o) {
        if (o == null)
            return null;
    }
}
