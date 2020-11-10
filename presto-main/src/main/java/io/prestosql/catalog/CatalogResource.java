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

import static io.prestosql.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.airlift.log.Logger;
import io.prestosql.catalog.utils.JsonUtil;
import io.prestosql.server.security.ResourceSecurity;
import org.apache.zookeeper.CreateMode;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/03 9:55
 * @since 1.0.0
 */
@Path("/v1/catalog")
public class CatalogResource {

    private static final Logger log = Logger.get(CatalogResource.class);

    private final DynamicCatalogStoreConfig config;
    private final String catalogZkPath;
    private final boolean enabledDynamic;

    @Inject
    public CatalogResource(DynamicCatalogStoreConfig config) {
        this.config = config;
        this.catalogZkPath = config.getCatalogZkPath();
        this.enabledDynamic = config.getDynamicEnabled();
    }

    private void check() {
        if (!enabledDynamic) {
            throw DynamicCatalogException.newInstance("please set catalog.dynamic.enabled=true in node.properties");
        }
    }

    @GET
    @Path("/test")
    @ResourceSecurity(PUBLIC)
    public Response test() {
        return Response.ok("hello catalog").build();
    }

    @GET
    @ResourceSecurity(PUBLIC)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<CatalogInfo> list() {
        check();
        try {
            List<String> list = config.getCuratorFramework().getChildren().forPath(catalogZkPath);
            return list.stream()
                    .map(s -> {
                        try {
                            byte[] bytes = config.getCuratorFramework().getData().forPath(catalogZkPath + "/" + s);
                            return JsonUtil.toObj(new String(bytes, StandardCharsets.UTF_8), CatalogInfo.class);
                        } catch (Exception e) {
                            log.error("get catalog [%s] error", s);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("failed get catalog list", e);
            return Collections.emptyList();
        }
    }

    @GET
    @Path("/{catalogName}")
    @ResourceSecurity(PUBLIC)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public CatalogInfo detail(@PathParam("catalogName") String catalogName) {
        check();
        try {
            byte[] bytes = config.getCuratorFramework().getData().forPath(catalogZkPath + "/" + catalogName);
            return JsonUtil.toObj(new String(bytes, StandardCharsets.UTF_8), CatalogInfo.class);
        } catch (Exception e) {
            log.error("failed get catalog detail");
            throw DynamicCatalogException.newInstance("failed get catalog detail", e);
        }
    }

    /**
     * 保存catalog
     * 新增或更新
     *
     * @param catalogInfo CatalogInfo
     */
    @POST
    @ResourceSecurity(PUBLIC)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void save(CatalogInfo catalogInfo) {
        check();
        try {
            requireNonNull(catalogInfo.getCatalogName(), "catalog can not be null");
            requireNonNull(catalogInfo.getConnectorName(), "connectorName can not be null");
            requireNonNull(catalogInfo.getProperties(), "properties can not be null");
            if (catalogInfo.getProperties().isEmpty()) {
                throw DynamicCatalogException.newInstance("catalog properties is null");
            }
            config.getCuratorFramework().create().orSetData().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(catalogZkPath + "/" + catalogInfo.getCatalogName(),
                            JsonUtil.toJson(catalogInfo).getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw DynamicCatalogException.newInstance(Objects.requireNonNullElse(e.getCause(), e).getMessage(), e);
        }
    }

    @DELETE
    @Path("{catalogName}")
    @ResourceSecurity(PUBLIC)
    public void delete(@PathParam("catalogName") String catalogName) {
        check();
        try {
            config.getCuratorFramework().delete().guaranteed().deletingChildrenIfNeeded()
                    .withVersion(-1)
                    .forPath(catalogZkPath + "/" + catalogName);
        } catch (Exception e) {
            throw DynamicCatalogException.newInstance(Objects.requireNonNullElse(e.getCause(), e).getMessage(), e);
        }
    }
}
