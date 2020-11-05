package io.prestosql.catalog.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import io.airlift.log.Logger;
import io.prestosql.catalog.DynamicCatalogException;
import io.prestosql.catalog.DynamicCatalogStoreConfig;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/05 15:36
 * @since 1.0.0
 */
public class JsonUtil {

    private static final Logger log = Logger.get(DynamicCatalogStoreConfig.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonUtil() {
        throw new IllegalStateException("util class");
    }

    public static <T> T toObj(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            log.error("toObj error");
            throw DynamicCatalogException.newInstance("string to object error", e);
        }
    }

    public static <T> T toObjNoException(T json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue((String) json, clazz);
        } catch (IOException e) {
            log.warn("toObjNoException error");
            return json;
        }
    }

    public static <T> T toObj(String json, TypeReference<T> typeReference) {
        try {
            return OBJECT_MAPPER.readValue(json, typeReference);
        } catch (IOException e) {
            log.error("toObj error");
            throw DynamicCatalogException.newInstance("string to object error", e);
        }
    }

    public static <T> List<T> toArray(String json, Class<T> clazz) {
        try {
            CollectionType type = OBJECT_MAPPER.getTypeFactory().constructCollectionType(ArrayList.class, clazz);
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            log.error("toArray error");
            throw DynamicCatalogException.newInstance("string to list error", e);
        }
    }

    public static <T> String toJson(T obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (IOException e) {
            log.error("toJson error");
            throw DynamicCatalogException.newInstance("object to string error", e);
        }
    }
}
