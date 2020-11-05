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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/29 17:30
 * @since 1.0.0
 */
public class CatalogInfo {

    private final String catalogName;
    private final String connectorName;
    private final Map<String, String> properties;

    @JsonCreator
    public CatalogInfo(@JsonProperty("catalogName") String catalogName,
                       @JsonProperty("connectorName") String connectorName,
                       @JsonProperty("properties") Map<String, String> properties) {
        this.catalogName = catalogName;
        this.connectorName = connectorName;
        this.properties = properties;
    }

    @JsonProperty
    public String getCatalogName() {
        return catalogName;
    }

    @JsonProperty
    public String getConnectorName() {
        return connectorName;
    }

    @JsonProperty
    public Map<String, String> getProperties() {
        return properties;
    }

}
