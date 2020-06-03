/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreSnapshotFile {

    private static final Logger LOG = LoggerFactory.getLogger(StoreSnapshotFile.class);

    private String path;

    public StoreSnapshotFile(String path) {
        super();
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }

    /**
     * Save value to snapshot file.
     */
    public boolean save(final Map<Integer, Object> map) {
        String json = JsonUtil.toJson(map);
        try {
            FileUtils.writeStringToFile(new File(path), json);
            return true;
        } catch (IOException e) {
            LOG.error("Fail to save snapshot", e);
            return false;
        }
    }

    public Map<Integer, Object> load() throws IOException {
        final String json = FileUtils.readFileToString(new File(path));
        if (!StringUtils.isBlank(json)) {
            return JsonUtil.convertMap(json, Integer.class, Object.class);
        }
        throw new IOException("Fail to load snapshot from " + path + ",content: " + json);
    }
}
