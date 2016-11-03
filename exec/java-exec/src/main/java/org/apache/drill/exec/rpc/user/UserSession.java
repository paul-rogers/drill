/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.user;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerUtil;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.proto.UserProtos.Property;
import org.apache.drill.exec.proto.UserProtos.UserProperties;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.SessionOptionManager;

import com.google.common.collect.Maps;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.util.BiConsumer;

public class UserSession implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserSession.class);

  public static final String SCHEMA = "schema";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String IMPERSONATION_TARGET = "impersonation_target";

  // known property names in lower case
  private static final Set<String> knownProperties = ImmutableSet.of(SCHEMA, USER, PASSWORD, IMPERSONATION_TARGET);

  private boolean supportComplexTypes = false;
  private UserCredentials credentials;
  private Map<String, String> properties;
  private OptionManager sessionOptions;
  private final AtomicInteger queryCount;

  /** Unique session identifier used as suffix in temporary table names. */
  private final String uuid;
  /** Cache that stores all temporary tables by schema names. */
  private final TemporaryTablesCache temporaryTablesCache;

  /** On session close drops all temporary tables from their schemas and clears temporary tables cache. */
  @Override
  public void close() {
    temporaryTablesCache.removeAll(new BiConsumer<AbstractSchema, String>() {
      @Override
      public void accept(AbstractSchema schema, String tableName) {
        try {
          if (schema.isAccessible() && schema.getTable(tableName) != null) {
            schema.dropTable(tableName);
            logger.info("Temporary table [{}] was dropped from schema [{}]", tableName, schema.getFullSchemaName());
          }
        } catch (Exception e) {
          logger.info("Problem during temporary table [{}] drop from schema [{}]",
                  tableName, schema.getFullSchemaName(), e);
        }
      }
    });
  }

  /**
   * Implementations of this interface are allowed to increment queryCount.
   * {@link org.apache.drill.exec.work.user.UserWorker} should have a member that implements the interface.
   * No other core class should implement this interface. Test classes may implement (see ControlsInjectionUtil).
   */
  public interface QueryCountIncrementer {
    void increment(final UserSession session);
  }

  public static class Builder {
    UserSession userSession;

    public static Builder newBuilder() {
      return new Builder();
    }

    public Builder withCredentials(UserCredentials credentials) {
      userSession.credentials = credentials;
      return this;
    }

    public Builder withOptionManager(OptionManager systemOptions) {
      userSession.sessionOptions = new SessionOptionManager(systemOptions, userSession);
      return this;
    }

    public Builder withUserProperties(UserProperties properties) {
      userSession.properties = Maps.newHashMap();
      if (properties != null) {
        for (int i = 0; i < properties.getPropertiesCount(); i++) {
          final Property property = properties.getProperties(i);
          final String propertyName = property.getKey().toLowerCase();
          if (knownProperties.contains(propertyName)) {
            userSession.properties.put(propertyName, property.getValue());
          } else {
            logger.warn("Ignoring unknown property: {}", propertyName);
          }
        }
      }
      return this;
    }

    public Builder setSupportComplexTypes(boolean supportComplexTypes) {
      userSession.supportComplexTypes = supportComplexTypes;
      return this;
    }

    public UserSession build() {
      UserSession session = userSession;
      userSession = null;
      return session;
    }

    Builder() {
      userSession = new UserSession();
    }
  }

  private UserSession() {
    queryCount = new AtomicInteger(0);
    uuid = UUID.randomUUID().toString();
    temporaryTablesCache = new TemporaryTablesCache(uuid);
  }

  public boolean isSupportComplexTypes() {
    return supportComplexTypes;
  }

  public OptionManager getOptions() {
    return sessionOptions;
  }

  public UserCredentials getCredentials() {
    return credentials;
  }

  /**
   * Replace current user credentials with the given user's credentials. Meant to be called only by a
   * {@link InboundImpersonationManager impersonation manager}.
   *
   * @param impersonationManager impersonation manager making this call
   * @param newCredentials user credentials to change to
   */
  public void replaceUserCredentials(final InboundImpersonationManager impersonationManager,
                                     final UserCredentials newCredentials) {
    Preconditions.checkNotNull(impersonationManager, "User credentials can only be replaced by an" +
        " impersonation manager.");
    credentials = newCredentials;
  }

  public String getTargetUserName() {
    return properties.get(IMPERSONATION_TARGET);
  }

  public String getDefaultSchemaName() {
    return getProp(SCHEMA);
  }

  public void incrementQueryCount(final QueryCountIncrementer incrementer) {
    assert incrementer != null;
    queryCount.incrementAndGet();
  }

  public int getQueryCount() {
    return queryCount.get();
  }

  /**
   * Update the schema path for the session.
   * @param newDefaultSchemaPath New default schema path to set. It could be relative to the current default schema or
   *                             absolute schema.
   * @param currentDefaultSchema Current default schema.
   * @throws ValidationException If the given default schema path is invalid in current schema tree.
   */
  public void setDefaultSchemaPath(String newDefaultSchemaPath, SchemaPlus currentDefaultSchema)
      throws ValidationException {
    final List<String> newDefaultPathAsList = Lists.newArrayList(newDefaultSchemaPath.split("\\."));
    SchemaPlus newDefault;

    // First try to find the given schema relative to the current default schema.
    newDefault = SchemaUtilites.findSchema(currentDefaultSchema, newDefaultPathAsList);

    if (newDefault == null) {
      // If we fail to find the schema relative to current default schema, consider the given new default schema path as
      // absolute schema path.
      newDefault = SchemaUtilites.findSchema(currentDefaultSchema, newDefaultPathAsList);
    }

    if (newDefault == null) {
      SchemaUtilites.throwSchemaNotFoundException(currentDefaultSchema, newDefaultSchemaPath);
    }

    setProp(SCHEMA, SchemaUtilites.getSchemaPath(newDefault));
  }

  /**
   * @return Get current default schema path.
   */
  public String getDefaultSchemaPath() {
    return getProp(SCHEMA);
  }

  /**
   * Get default schema from current default schema path and given schema tree.
   * @param rootSchema root schema
   * @return A {@link org.apache.calcite.schema.SchemaPlus} object.
   */
  public SchemaPlus getDefaultSchema(SchemaPlus rootSchema) {
    final String defaultSchemaPath = getProp(SCHEMA);

    if (Strings.isNullOrEmpty(defaultSchemaPath)) {
      return null;
    }

    return SchemaUtilites.findSchema(rootSchema, defaultSchemaPath);
  }

  public boolean setSessionOption(String name, String value) {
    return true;
  }

  /**
   * @return unique session identifier
   */
  public String getUuid() { return uuid; }

  /**
   * Adds temporary table to temporary tables cache.
   *
   * @param schema table schema
   * @param tableName original table name
   * @return generated temporary table name
   */
  public String registerTemporaryTable(AbstractSchema schema, String tableName) {
    return temporaryTablesCache.add(schema, tableName);
  }

  /**
   * Looks for temporary table in temporary tables cache by its name in specified schema.
   *
   * @param fullSchemaName table full schema name (example, dfs.tmp)
   * @param tableName original table name
   * @return temporary table name if found, null otherwise
   */
  public String findTemporaryTable(String fullSchemaName, String tableName) {
    return temporaryTablesCache.find(fullSchemaName, tableName);
  }

  /**
   * Before removing temporary table from temporary tables cache,
   * checks if table exists physically on disk, if yes, removes it.
   *
   * @param fullSchemaName full table schema name (example, dfs.tmp)
   * @param tableName original table name
   * @return true if table was physically removed, false otherwise
   */
  public boolean removeTemporaryTable(String fullSchemaName, String tableName) {
    final AtomicBoolean result = new AtomicBoolean();
    temporaryTablesCache.remove(fullSchemaName, tableName, new BiConsumer<AbstractSchema, String>() {
      @Override
      public void accept(AbstractSchema schema, String temporaryTableName) {
        if (schema.getTable(temporaryTableName) != null) {
          schema.dropTable(temporaryTableName);
          result.set(true);
        }
      }
    });
    return result.get();
  }

  private String getProp(String key) {
    return properties.get(key) != null ? properties.get(key) : "";
  }

  private void setProp(String key, String value) {
    properties.put(key, value);
  }

  /**
   * Temporary tables cache stores data by full schema name (schema and workspace separated by dot
   * (example: dfs.tmp)) as key, and map of generated temporary tables names
   * and its schemas represented by {@link AbstractSchema} as values.
   * Schemas represented by {@link AbstractSchema} are used to drop temporary tables.
   * Generated temporary tables consists of original table name and unique session id.
   * Cache is represented by {@link ConcurrentMap} so if is thread-safe and can be used
   * in multi-threaded environment.
   *
   * Temporary tables cache is used to find temporary table by its name and schema,
   * to drop all existing temporary tables on session close
   * or remove temporary table from cache on user demand.
   */
  public static class TemporaryTablesCache {

    private final String uuid;
    private final ConcurrentMap<String, ConcurrentMap<String, AbstractSchema>> temporaryTables;

    public TemporaryTablesCache(String uuid) {
      this.uuid = uuid;
      this.temporaryTables = Maps.newConcurrentMap();
    }

    /**
     * Generates temporary table name using its original table name and unique session identifier.
     * Caches generated table name and its schema in temporary table cache.
     *
     * @param schema table schema
     * @param tableName original table name
     * @return generated temporary table name
     */
    public String add(AbstractSchema schema, String tableName) {
      final String temporaryTableName = SqlHandlerUtil.generateTemporaryTableName(tableName, uuid);
      final ConcurrentMap<String, AbstractSchema> newValues = Maps.newConcurrentMap();
      newValues.put(temporaryTableName, schema);
      final ConcurrentMap<String, AbstractSchema> oldValues = temporaryTables.putIfAbsent(schema.getFullSchemaName(), newValues);
      if (oldValues != null) {
        oldValues.putAll(newValues);
      }
      return temporaryTableName;
    }

    /**
     * Looks for temporary table in temporary tables cache.
     *
     * @param fullSchemaName table schema name which is used as key in temporary tables cache
     * @param tableName original table name
     * @return generated temporary table name (original name with unique session id) if table is found,
     *         null otherwise
     */
    public String find(String fullSchemaName, String tableName) {
      final String temporaryTableName = SqlHandlerUtil.generateTemporaryTableName(tableName, uuid);
      final ConcurrentMap<String, AbstractSchema> tables = temporaryTables.get(fullSchemaName);
      if (tables == null || tables.get(temporaryTableName) == null) {
        return null;
      }
      return temporaryTableName;
    }

    /**
     * Removes temporary table from temporary tables cache
     * by temporary table original name and its schema.
     *
     * @param fullSchemaName table schema name which is used as key in temporary tables cache
     * @param tableName original table name
     * @param action action applied to temporary table and its schema before removing from cache
     */
    public void remove(String fullSchemaName, String tableName, BiConsumer<AbstractSchema, String> action) {
      final String temporaryTableName = SqlHandlerUtil.generateTemporaryTableName(tableName, uuid);
      final ConcurrentMap<String, AbstractSchema> tables = temporaryTables.get(fullSchemaName);
      AbstractSchema schema;
      if (tables != null && (schema = tables.get(temporaryTableName)) != null) {
        if (action != null) {
          action.accept(schema, temporaryTableName);
        }
        tables.remove(temporaryTableName);
      }
    }

    /**
     * Removes all temporary tables from temporary tables cache.
     * Before clearing cache applies table passed action to each table.
     *
     * @param action action applied to temporary table and its schema before cache clean up
     */
    public void removeAll(BiConsumer<AbstractSchema, String> action) {
      if (action != null) {
        for (Map.Entry<String, ConcurrentMap<String, AbstractSchema>> schemaEntry : temporaryTables.entrySet()) {
          for (Map.Entry<String, AbstractSchema> tableEntry : schemaEntry.getValue().entrySet()) {
            action.accept(tableEntry.getValue(), tableEntry.getKey());
          }
        }
      }
      temporaryTables.clear();
    }
  }

}
