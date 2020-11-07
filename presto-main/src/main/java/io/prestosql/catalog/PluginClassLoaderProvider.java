package io.prestosql.catalog;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/08 3:00
 * @since 1.0.0
 */
public class PluginClassLoaderProvider {

    private PluginClassLoaderProvider() {
    }

    private static final Map<String, ClassLoader> PLUGIN_CLASS_LOADER_MAP = new ConcurrentHashMap<>();

    public static void put(String key, ClassLoader classLoader) {
        PLUGIN_CLASS_LOADER_MAP.put(key, classLoader);
    }

    public static ClassLoader get(String key) {
       return PLUGIN_CLASS_LOADER_MAP.get(key);
    }
}
