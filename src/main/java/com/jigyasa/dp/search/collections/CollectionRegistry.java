package com.jigyasa.dp.search.collections;

import com.jigyasa.dp.search.handlers.IndexWriterManagerISCH;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.models.ServerMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

/**
 * Registry for managing multiple collection contexts.
 * Each collection has its own Lucene index, schema, and lifecycle.
 *
 * Collections must be explicitly created via {@link #createCollection}.
 * The "default" collection is created at startup time.
 * No auto-creation — requests to non-existent collections fail fast.
 *
 * Thread-safe: uses ConcurrentHashMap with explicit synchronization for create/close.
 */
public class CollectionRegistry {
    private static final Logger log = LoggerFactory.getLogger(CollectionRegistry.class);
    public static final String DEFAULT_COLLECTION = "default";

    /** Only alphanumeric, hyphens, underscores. 1-64 chars. */
    private static final Pattern VALID_NAME = Pattern.compile("^[a-zA-Z0-9_-]{1,64}$");

    private final ConcurrentMap<String, CollectionContext> collections = new ConcurrentHashMap<>();
    private final String baseIndexDir;
    private final String baseTranslogDir;
    private final ServerMode serverMode;

    public CollectionRegistry(String baseIndexDir, String baseTranslogDir, ServerMode serverMode) {
        this.baseIndexDir = baseIndexDir;
        this.baseTranslogDir = baseTranslogDir;
        this.serverMode = serverMode;
    }

    /**
     * Initialize the registry by creating and starting the default collection.
     */
    public void initialize(IndexSchema schema) {
        createCollection(DEFAULT_COLLECTION, schema);
        log.info("CollectionRegistry initialized with default collection");
    }

    /**
     * Resolve collection name: empty/null maps to "default".
     */
    public static String resolveCollectionName(String collection) {
        return (collection == null || collection.isEmpty()) ? DEFAULT_COLLECTION : collection;
    }

    /**
     * Create a new collection with the given schema.
     * @throws IllegalArgumentException if name is invalid or collection already exists
     * @throws IllegalStateException if resource initialization fails
     */
    public synchronized void createCollection(String name, IndexSchema schema) {
        String resolved = resolveCollectionName(name);
        validateCollectionName(resolved);

        if (collections.containsKey(resolved)) {
            throw new IllegalArgumentException("Collection '" + resolved + "' already exists");
        }

        CollectionContext ctx = null;
        try {
            ctx = new CollectionContext(resolved, serverMode,
                    Path.of(baseIndexDir, resolved).toString(),
                    Path.of(baseTranslogDir, resolved).toString());
            ctx.initialize(schema);
            collections.put(resolved, ctx);
            log.info("Collection '{}' created", resolved);
        } catch (Exception e) {
            // Clean up partially initialized resources
            if (ctx != null) {
                try { ctx.shutdown(); } catch (Exception se) {
                    log.warn("Failed to clean up after failed collection creation", se);
                }
            }
            throw new IllegalStateException("Failed to create collection '" + resolved + "': " + e.getMessage(), e);
        }
    }

    /**
     * Close a collection — releases all resources but preserves data on disk.
     * @throws IllegalArgumentException if collection doesn't exist
     */
    public synchronized void closeCollection(String name) {
        String resolved = resolveCollectionName(name);
        CollectionContext ctx = collections.remove(resolved);
        if (ctx == null) {
            throw new IllegalArgumentException("Collection '" + resolved + "' does not exist");
        }
        ctx.shutdown();
        log.info("Collection '{}' closed", resolved);
    }

    /**
     * Reopen a previously closed collection.
     * If schema is provided, uses it. Otherwise, reads persisted schema from the Lucene index.
     * @throws IllegalArgumentException if collection name invalid, already open, or no schema available
     */
    public void openCollection(String name, IndexSchema schema) {
        String resolved = resolveCollectionName(name);
        IndexSchema effectiveSchema = schema;
        if (effectiveSchema == null) {
            // Try reading persisted schema from the index on disk
            String indexDir = Path.of(baseIndexDir, resolved).toString();
            effectiveSchema = IndexWriterManagerISCH.readPersistedSchema(indexDir);
            if (effectiveSchema == null) {
                throw new IllegalArgumentException(
                        "No schema provided and no persisted schema found for collection '" + resolved + "'");
            }
            log.info("Using persisted schema for collection '{}'", resolved);
        }
        createCollection(resolved, effectiveSchema);
    }

    /**
     * Get HandlerHelpers for an existing collection.
     * @throws IllegalArgumentException if collection doesn't exist
     */
    public HandlerHelpers resolveHelpers(String collection) {
        String resolved = resolveCollectionName(collection);
        CollectionContext ctx = collections.get(resolved);
        if (ctx == null) {
            throw new IllegalArgumentException("Collection '" + resolved + "' does not exist. Create it first.");
        }
        return ctx.getHelpers();
    }

    /**
     * Get IndexSchemaManager for an existing collection.
     * @throws IllegalArgumentException if collection doesn't exist
     */
    public IndexSchemaManager resolveSchemaManager(String collection) {
        String resolved = resolveCollectionName(collection);
        CollectionContext ctx = collections.get(resolved);
        if (ctx == null) {
            throw new IllegalArgumentException("Collection '" + resolved + "' does not exist. Create it first.");
        }
        return ctx.getSchemaManager();
    }

    /**
     * Check if a collection exists.
     */
    public boolean exists(String collection) {
        return collections.containsKey(resolveCollectionName(collection));
    }

    /**
     * Get all active collection names.
     */
    public Collection<String> listCollections() {
        return collections.keySet();
    }

    /**
     * Get health info for all active collections.
     */
    public java.util.List<com.jigyasa.dp.search.protocol.CollectionHealth> getHealthForAll() {
        return collections.values().stream()
                .map(CollectionContext::getHealth)
                .toList();
    }

    /**
     * Shutdown all collections. Called during server shutdown.
     */
    public void shutdownAll() {
        log.info("Shutting down all collections: {}", collections.keySet());
        for (CollectionContext ctx : collections.values()) {
            ctx.shutdown();
        }
        collections.clear();
    }

    /**
     * Validate collection name against path traversal and naming rules.
     */
    private static void validateCollectionName(String name) {
        if (!VALID_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "Invalid collection name '" + name + "'. " +
                    "Must be 1-64 characters, alphanumeric, hyphens, or underscores only.");
        }
    }
}
