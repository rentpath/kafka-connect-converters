package com.rentpath.kafka.connect.converters.edn;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import us.bpsm.edn.Keyword;

import java.util.*;

public class KeywordFieldSchemaBuilder extends SchemaBuilder {
    private Map<String, KeywordField> keywordFields = null;

    public KeywordFieldSchemaBuilder(Type type) {
        super(type);
        if (type() == Type.STRUCT) {
            this.keywordFields = new LinkedHashMap();
        }
    }

    public SchemaBuilder keywordField(Keyword fieldKeyword, String fieldName, Schema fieldSchema) {
        this.field(fieldName, fieldSchema);
        int fieldIndex = this.keywordFields.size();
        this.keywordFields.put(fieldName, new KeywordField(fieldKeyword, fieldName, fieldIndex, fieldSchema));
        return this;
    }

    public List<Field> keywordFields() {
        if (type() != Type.STRUCT) {
            throw new DataException("Cannot list fields on non-struct type");
        } else {
            return new ArrayList<Field>(this.keywordFields.values());
        }
    }

    public static KeywordFieldSchemaBuilder struct() {
        return new KeywordFieldSchemaBuilder(Type.STRUCT);
    }

    @Override
    public Schema build() {
        return new ConnectSchema(type(), this.isOptional(), defaultValue(), name(), version(), doc(), parameters() == null ? null : Collections.unmodifiableMap(parameters()), this.keywordFields == null ? null : Collections.unmodifiableList(new ArrayList(this.keywordFields.values())), keySchema(), valueSchema());
    }
}
