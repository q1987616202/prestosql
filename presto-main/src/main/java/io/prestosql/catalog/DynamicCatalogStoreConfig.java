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

import java.util.Objects;

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
    private String dynamicEnabled;
    private String zkAddress;
    private String nodePath;
    private String namespace;
    private CuratorFramework curatorFramework;

    @Config("catalog.dynamic.enabled")
    public DynamicCatalogStoreConfig setDynamicEnabled(String dynamicEnabled) {
        this.dynamicEnabled = dynamicEnabled;
        return this;
    }

    public boolean getDynamicEnabled() {
        return Boolean.parseBoolean(Objects.requireNonNullElse(dynamicEnabled, "true"));
    }

    @Config("catalog.zk.address")
    public DynamicCatalogStoreConfig setCatalogZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
        return this;
    }

    public String getCatalogZkAddress() {
        return Objects.requireNonNullElse(zkAddress, "127.0.0.1:2181");
    }

    @Config("catalog.zk.namespace")
    public DynamicCatalogStoreConfig setCatalogZkNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public String getCatalogZkNamespace() {
        return Objects.requireNonNullElse(namespace, "presto");
    }

    @Config("catalog.zk.path")
    public DynamicCatalogStoreConfig setCatalogZkPath(String nodePath) {
        this.nodePath = nodePath;
        return this;
    }

    public String getCatalogZkPath() {
        return Objects.requireNonNullElse(nodePath, "/catalog/meta");
    }

    public CuratorFramework getCuratorFramework() {
        createCuratorFramework();
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
