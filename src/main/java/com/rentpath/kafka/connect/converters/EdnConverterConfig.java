package com.rentpath.kafka.connect.converters;

import com.google.common.base.CaseFormat;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.storage.ConverterConfig;

import java.util.Map;

public class EdnConverterConfig extends ConverterConfig {
    private final static ConfigDef CONFIG;

    public static final String MAP_KEYS_KEYWORDIZE_CONFIG = "map.keys.keywordize";
    public static final boolean MAP_KEYS_KEYWORDIZE_DEFAULT = true;
    private static final String MAP_KEYS_KEYWORDIZE_DOC = "Whether to keywordize string map keys internally";
    private static final String MAP_KEYS_KEYWORDIZE_DISPLAY = "Keywordize Map Keys";

    public static final String MAP_KEYS_INTERNAL_FORMAT_CONFIG = "map.keys.internal.format";
    public static final String MAP_KEYS_INTERNAL_FORMAT_DEFAULT = "kebab";
    private static final String MAP_KEYS_INTERNAL_FORMAT_DOC = "Format to use for map keys internal to the kafka ecosystem. May be one of 'kebab' (default), 'underscore', or 'camel'.";
    private static final String MAP_KEYS_INTERNAL_FORMAT_DISPLAY = "Internal Map Key Format";

    public static final String MAP_KEYS_EXTERNAL_FORMAT_CONFIG = "map.keys.original.format";
    public static final String MAP_KEYS_EXTERNAL_FORMAT_DEFAULT = "underscore";
    private static final String MAP_KEYS_EXTERNAL_FORMAT_DOC = "Format to use for map keys outside of the kafka ecosystem. May be one of 'underscore' (default) or 'camel'.";
    private static final String MAP_KEYS_EXTERNAL_FORMAT_DISPLAY = "External Map Key Format";

    public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    private static final String SCHEMAS_CACHE_SIZE_DOC = "The maximum number of schemas that can be cached in this converter instance.";
    private static final String SCHEMAS_CACHE_SIZE_DISPLAY = "Schema Cache Size";

    static {
        CONFIG = ConverterConfig.newConfigDef();
        int orderInGroup = 0;
        CONFIG.define(
                SCHEMAS_CACHE_SIZE_CONFIG,
                Type.INT,
                SCHEMAS_CACHE_SIZE_DEFAULT,
                Importance.HIGH,
                SCHEMAS_CACHE_SIZE_DOC,
                "Schemas",
                orderInGroup++,
                Width.MEDIUM,
                SCHEMAS_CACHE_SIZE_DISPLAY
        );
        CONFIG.define(
                MAP_KEYS_EXTERNAL_FORMAT_CONFIG,
                Type.STRING,
                MAP_KEYS_EXTERNAL_FORMAT_DEFAULT,
                Importance.HIGH,
                MAP_KEYS_EXTERNAL_FORMAT_DOC,
                "Map Key Interpretation",
                orderInGroup++,
                Width.MEDIUM,
                MAP_KEYS_EXTERNAL_FORMAT_DISPLAY
        );
        CONFIG.define(
                MAP_KEYS_INTERNAL_FORMAT_CONFIG,
                Type.STRING,
                MAP_KEYS_INTERNAL_FORMAT_DEFAULT,
                Importance.HIGH,
                MAP_KEYS_INTERNAL_FORMAT_DOC,
                "Map Key Interpretation",
                orderInGroup++,
                Width.MEDIUM,
                MAP_KEYS_INTERNAL_FORMAT_DISPLAY
        );
        CONFIG.define(
                MAP_KEYS_KEYWORDIZE_CONFIG,
                Type.BOOLEAN,
                MAP_KEYS_KEYWORDIZE_DEFAULT,
                Importance.HIGH,
                MAP_KEYS_KEYWORDIZE_DOC,
                "Map Key Interpretation",
                orderInGroup++,
                Width.MEDIUM,
                MAP_KEYS_KEYWORDIZE_DISPLAY
        );
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public EdnConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

    public int schemaCacheSize() {
        return getInt(SCHEMAS_CACHE_SIZE_CONFIG);
    }

    private CaseFormat getFormat(String v) {
        switch (v) {
            case "camel":
                return CaseFormat.UPPER_CAMEL;
            case "underscore":
                return CaseFormat.LOWER_UNDERSCORE;
        }
        return CaseFormat.LOWER_HYPHEN;
    }

    public CaseFormat externalKeyFormat() {
        return getFormat(getString(MAP_KEYS_EXTERNAL_FORMAT_CONFIG));
    }

    public CaseFormat internalKeyFormat() {
        return getFormat(getString(MAP_KEYS_INTERNAL_FORMAT_CONFIG));
    }

    public boolean keywordizeKeys() {
        return getBoolean(MAP_KEYS_KEYWORDIZE_CONFIG);
    }
}
