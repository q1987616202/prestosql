/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.catalog;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.configuration.Config;
import io.airlift.log.Logger;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/29 17:45
 * @since 1.0.0
 */
public class DynamicCatalogStoreConfig {

    private static final Logger log = Logger.get(DynamicCatalogStoreConfig.class);
    private String zkAddress;
    private String nodePath;
    private String namespace;
    private String restartCommand;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private CuratorFramework curatorFramework;


    @Config("catalog.zk.address")
    public DynamicCatalogStoreConfig setCatalogZkAddress(String zkAddress) {
        this.zkAddress = zkAddress != null && !"".equals(zkAddress.trim()) ? zkAddress : "127.0.0.1:2181";
        return this;
    }

    public String getCatalogZkAddress() {
        return zkAddress;
    }

    @Config("catalog.zk.namespace")
    public DynamicCatalogStoreConfig setCatalogZkNamespace(String namespace) {
        this.namespace = namespace != null && !"".equals(namespace.trim()) ? namespace : "presto";
        return this;
    }

    public String getCatalogZkNamespace() {
        return namespace;
    }

    @Config("catalog.zk.path")
    public DynamicCatalogStoreConfig setCatalogZkPath(String nodePath) {
        this.nodePath = nodePath != null && !"".equals(nodePath.trim()) ? nodePath : "/catalog/meta";
        return this;
    }

    @Config("cluster.restart.command")
    public DynamicCatalogStoreConfig setRestartCommand(String restartCommand) {
        this.restartCommand  = restartCommand;
        return this;
    }

    public String getRestartCommand() {
        return restartCommand;
    }

    public String getCatalogZkPath() {
        return nodePath;
    }

    public Map<String, CatalogInfo> getCatalogInfoMap() {
        log.info("loading catalog from zookeeper");
        return getCatalogInfoFromZk();
    }

    private Map<String, CatalogInfo> getCatalogInfoFromZk() {
        createCuratorFramework();
        Map<String, CatalogInfo> map = new HashMap<>(16);
        try {
            List<String> children = curatorFramework.getChildren().forPath(nodePath);
            for (String catalogPath : children) {
                byte[] bytes = curatorFramework.getData().forPath(nodePath + "/" + catalogPath);
                if (bytes == null) {
                    log.info("node path %s has no data", catalogPath);
                    continue;
                }
                CatalogInfo catalogInfo = objectMapper.readValue(bytes, CatalogInfo.class);
                map.put(catalogPath, catalogInfo);
            }
        } catch (Exception e) {
            throw DynamicCatalogException.newInstance("get catalog from zk error", e);
        }
        return map;
    }

    public CuratorFramework getCuratorFramework() {
        return curatorFramework;
    }

    private synchronized void createCuratorFramework() {
        if (Objects.isNull(curatorFramework)) {
            requireNonNull(zkAddress, "zookeeper address is null");
            requireNonNull(nodePath, "zookeeper nodePath info is null");
            log.info("get catalog from zookeeper, address: %s, node: %s ", zkAddress, nodePath);
            RetryPolicy backoffRetry = new ExponentialBackoffRetry(3000, 2);
            curatorFramework = CuratorFrameworkFactory.builder()
                    .connectString(zkAddress)
                    .sessionTimeoutMs(30000)
                    .connectionTimeoutMs(30000)
                    .retryPolicy(backoffRetry)
                    .namespace(namespace)
                    .build();
            curatorFramework.start();
        }
    }

}
