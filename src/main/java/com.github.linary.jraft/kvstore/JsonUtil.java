/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */

package com.github.linary.jraft.kvstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public final class JsonUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JsonUtil.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        SimpleModule module = new SimpleModule();
        registerModule(module);
    }

    public static void registerModule(Module module) {
        MAPPER.registerModule(module);
    }

    public static String toJson(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize objects", e);
            throw new RuntimeException("Failed to serialize objects", e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            LOG.error("Failed to deserialize json", e);
            throw new RuntimeException("Failed to deserialize json", e);
        }
    }

    public static <T> T fromJson(String json, TypeReference<T> typeRef) {
        try {
            return MAPPER.readValue(json, typeRef);
        } catch (IOException e) {
            LOG.error("Failed to deserialize json", e);
            throw new RuntimeException("Failed to deserialize json", e);
        }
    }

    public static <T> T convert(JsonNode node, Class<T> clazz) {
        return MAPPER.convertValue(node, clazz);
    }

    public static <T> Set<T> convertSet(String json, Class<T> clazz) {
        JavaType type = MAPPER.getTypeFactory().constructCollectionType(
                                                LinkedHashSet.class, clazz);
        try {
            return MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to deserialize json", e);
            throw new RuntimeException("Failed to deserialize json", e);
        }
    }

    public static <T> Set<T> convertSet(JsonNode node, Class<T> clazz) {
        JavaType type = MAPPER.getTypeFactory().constructCollectionType(
                                                LinkedHashSet.class, clazz);
        return MAPPER.convertValue(node, type);
    }

    public static <T> List<T> convertList(String json, Class<T> clazz) {
        JavaType type = MAPPER.getTypeFactory()
                              .constructCollectionType(ArrayList.class, clazz);
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException e) {
            LOG.error("Failed to deserialize json", e);
            throw new RuntimeException("Failed to deserialize json", e);
        }
    }

    public static <T> List<T> convertList(JsonNode node, Class<T> clazz) {
        JavaType type = MAPPER.getTypeFactory()
                              .constructCollectionType(List.class, clazz);
        return MAPPER.convertValue(node, type);
    }

    public static <K, V> Map<K, V> convertMap(String json, Class<K> kClazz,
                                              Class<V> vClazz) {
        JavaType type = MAPPER.getTypeFactory()
                              .constructMapType(Map.class, kClazz, vClazz);
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException e) {
            LOG.error("Failed to deserialize json", e);
            throw new RuntimeException("Failed to deserialize json", e);
        }
    }

    public static <K, V> Map<K, V> convertMap(JsonNode node, Class<K> kClazz,
                                              Class<V> vClazz) {
        JavaType type = MAPPER.getTypeFactory()
                              .constructMapType(Map.class, kClazz, vClazz);
        return MAPPER.convertValue(node, type);
    }
}
