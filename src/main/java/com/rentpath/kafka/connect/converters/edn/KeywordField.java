package com.rentpath.kafka.connect.converters.edn;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import us.bpsm.edn.Keyword;

public class KeywordField extends Field {
    private Keyword keyword;

    public KeywordField(Keyword keyword, String name, int index, Schema schema) {
        super(name, index, schema);
        this.keyword = keyword;
    }

    public Keyword keyword() {
        return keyword;
    }
}
