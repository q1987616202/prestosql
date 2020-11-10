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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.log.Logger;
import io.prestosql.connector.CatalogName;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.metadata.InternalNodeManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/29 17:43
 * @since 1.0.0
 */
public class DynamicCatalogStore {

    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private final DynamicCatalogStoreConfig dynamicCatalogStoreConfig;
    private final Announcer announcer;
    private final InternalNodeManager nodeManager;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager,
                               DynamicCatalogStoreConfig config,
                               Announcer announcer,
                               InternalNodeManager nodeManager) {
        this.connectorManager = connectorManager;
        this.dynamicCatalogStoreConfig = config;
        this.announcer = announcer;
        this.nodeManager = nodeManager;
    }

    public boolean areCatalogsLoaded() {
        return catalogsLoaded.get();
    }

    public boolean isEnabledDynamic() {
        return dynamicCatalogStoreConfig.getDynamicEnabled();
    }

    public void loadCatalogs() {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }
        // get catalog from zk
        load();
        catalogsLoaded.set(true);
    }

    public void load() {
        CuratorFramework curatorFramework = dynamicCatalogStoreConfig.getCuratorFramework();
        TreeCache cache = TreeCache
                .newBuilder(curatorFramework, dynamicCatalogStoreConfig.getCatalogZkPath())
                .setCacheData(false)
                .build();
        try {
            addLister(cache);
            cache.start();
        } catch (Exception e) {
            throw DynamicCatalogException.newInstance("zk pathChildrenCache error", e);
        }
    }

    private void addLister(TreeCache cache) {
        cache.getListenable().addListener((client, event) -> {
            switch (event.getType()) {
                case NODE_ADDED:
                    createCatalog(event.getData());
                    break;
                case NODE_UPDATED:
                    updateCatalog(event.getData(), event.getData());
                    break;
                case NODE_REMOVED:
                    deleteCatalog(event.getData());
                    break;
                default:
                    break;
            }
        });
    }

    private void updateCatalog(ChildData oldNode, ChildData node) {
        deleteCatalog(oldNode);
        createCatalog(node);
    }

    private void deleteCatalog(ChildData childData) {
        String catalogName = childData.getPath()
                .replace(dynamicCatalogStoreConfig.getCatalogZkPath() + "/", "");
        if (catalogName.contains(File.separator)) {
            return;
        }
        log.info("-- Removing catalog %s", catalogName);
        connectorManager.dropConnection(catalogName);
        log.info("-- Removed catalog %s", catalogName);
    }

    private void createCatalog(ChildData childData) {
        if (childData.getPath().equals(dynamicCatalogStoreConfig.getCatalogZkPath())) {
            return;
        }
        CatalogInfo catalogInfo = node2CatalogInfo(childData);
        log.info("-- Adding catalog %s", catalogInfo.getCatalogName());
        CatalogName connectorId;
        try {
            connectorId = loadCatalog(catalogInfo);
        } catch (Exception e) {
            log.warn("-- Add catalog error, %s", Objects.requireNonNullElse(e.getCause(), e).getMessage());
            return;
        }
        updateConnectorIdAnnouncement(announcer, connectorId, nodeManager);
    }

    private CatalogInfo node2CatalogInfo(ChildData childData) {
        try {
            return objectMapper.readValue(childData.getData(), CatalogInfo.class);
        } catch (IOException e) {
            log.error("dynamic catalog zk node data error");
            throw DynamicCatalogException.newInstance("dynamic catalog zk node data error", e);
        }
    }

    private CatalogName loadCatalog(CatalogInfo catalogInfo) {
        String catalogName = catalogInfo.getCatalogName();
        log.info("-- Loading catalog %s --", catalogName);
        Map<String, String> properties = catalogInfo.getProperties();
        String connectorName = catalogInfo.getConnectorName();
        checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", catalogInfo.getProperties());
        CatalogName connectorId = connectorManager.createCatalog(catalogName, connectorName, ImmutableMap.copyOf(properties));
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
        return connectorId;
    }

    @SuppressWarnings("DuplicatedCode")
    private static void updateConnectorIdAnnouncement(Announcer announcer, CatalogName connectorId, InternalNodeManager nodeManager) {
        /*
        This code was copied from Server, and is a hack that should be removed
        when the connectorId property is removed
         */

        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // update connectorIds property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(connectorId.toString());
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType())
                .addProperties(properties).build());

        announcer.forceAnnounce();
        nodeManager.refreshNodes();
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements) {
        for (ServiceAnnouncement announcement : announcements) {
            if ("presto".equals(announcement.getType())) {
                return announcement;
            }
        }
        throw DynamicCatalogException.newInstance(String.format("presto announcement not found: %s", announcements));
    }

}
