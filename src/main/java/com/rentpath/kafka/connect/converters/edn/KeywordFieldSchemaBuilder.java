package com.rentpath.kafka.connect.converters.edn;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.SchemaBuilderException;
import us.bpsm.edn.Keyword;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KeywordFieldSchemaBuilder extends SchemaBuilder {
    private Map<String, KeywordField> keywordFields = null;

    public KeywordFieldSchemaBuilder(Type type) {
        super(type);
        if (type() == Type.STRUCT) {
            this.keywordFields = new LinkedHashMap();
        }
    }

    public SchemaBuilder keywordField(Keyword fieldKeyword, String fieldName, Schema fieldSchema) {
        if (type() != Type.STRUCT) {
            throw new SchemaBuilderException("Cannot create fields on type " + type());
        } else if (null != fieldName && !fieldName.isEmpty()) {
            if (null == fieldSchema) {
                throw new SchemaBuilderException("fieldSchema for field " + fieldName + " cannot be null.");
            } else {
                int fieldIndex = this.keywordFields.size();
                if (this.keywordFields.containsKey(fieldName)) {
                    throw new SchemaBuilderException("Cannot create field because of field name duplication " + fieldName);
                } else {
                    this.keywordFields.put(fieldName, new KeywordField(fieldKeyword, fieldName, fieldIndex, fieldSchema));
                    return this;
                }
            }
        } else {
            throw new SchemaBuilderException("fieldName cannot be null.");
        }
    }

    public List<Field> fields() {
        if (type() != Type.STRUCT) {
            throw new DataException("Cannot list fields on non-struct type");
        } else {
            return new ArrayList(this.keywordFields.values());
        }
    }

    public static KeywordFieldSchemaBuilder struct() {
        return new KeywordFieldSchemaBuilder(Type.STRUCT);
    }
}
