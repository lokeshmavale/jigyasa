package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.mappers.FieldMapperStrategy;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.protocol.IndexResponse;
import com.jigyasa.dp.search.protocol.RefreshPolicy;
import com.jigyasa.dp.search.services.RequestHandlerBase;
import com.jigyasa.dp.search.utils.DocIdOverlapLock;
import com.jigyasa.dp.search.utils.SystemFields;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class IndexRequestHandler extends RequestHandlerBase<IndexRequest, IndexResponse> {
    private static final Logger log = LoggerFactory.getLogger(IndexRequestHandler.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final DocIdOverlapLock lock;
    private final CollectionRegistry registry;

    public IndexRequestHandler(DocIdOverlapLock lock, CollectionRegistry registry) {
        super("Index");
        this.lock = lock;
        this.registry = registry;
    }

    @Override
    public void internalHandle(IndexRequest req, StreamObserver<IndexResponse> observer) {
        HandlerHelpers handlerHelpers = registry.resolveHelpers(req.getCollection());

        IndexWriterManagerISCH indexWriterManager = null;
        try {
            indexWriterManager = handlerHelpers.getIndexWriterManager();
            final IndexWriter writer = indexWriterManager.acquireWriter();
            IndexSchema indexSchema = handlerHelpers.getIndexSchemaManager().getIndexSchema();
            // Index first, then persist to translog. If crash between index and translog,
            // uncommitted Lucene buffer is lost — acceptable (same as ES default durability).
            // Writing translog BEFORE would cause ghost data: if indexing fails (validation error),
            // client gets error, but recovery replays the entry → data appears that client was told failed.
            IndexResult result = processIndexRequests(req, indexSchema, writer, lock);
            handlerHelpers.getTranslogAppenderManager().getAppender().append(req);
            // NRT refresh based on client's requested policy
            RefreshPolicy refresh = req.getRefresh();
            if (refresh == RefreshPolicy.NONE) {
                // Fire-and-forget: best throughput for bulk ingestion
            } else if (refresh == RefreshPolicy.IMMEDIATE) {
                handlerHelpers.getIndexSearcherManager().forceRefresh();
            } else {
                // WAIT_FOR (default): block until docs are searchable
                handlerHelpers.getIndexSearcherManager().waitForGeneration(result.maxSeqNo());
            }
            observer.onNext(result.response());
            observer.onCompleted();
        } catch (Exception e) {
            log.error("Index request failed", e);
            // FQN required: io.grpc.Status collides with com.google.rpc.Status import
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        } finally {

            if (indexWriterManager != null) {
                indexWriterManager.releaseWriter();
            }
        }
    }

    public record IndexResult(IndexResponse response, long maxSeqNo) {}

    public static IndexResult processIndexRequests(IndexRequest req, IndexSchema indexSchema, IndexWriter indexWriter, DocIdOverlapLock lock) throws InterruptedException, TimeoutException {
        List<IndexRequestContext> requestContexts = getRequestContexts(req);
        Set<String> uniqueDocKeysForRequest = getUniqueDocKeysForRequest(requestContexts, indexSchema.getInitializedSchema());

        // Namespace keys with collection name to prevent cross-collection false contention
        String collection = com.jigyasa.dp.search.collections.CollectionRegistry
                .resolveCollectionName(req.getCollection());
        Set<String> namespacedKeys = uniqueDocKeysForRequest.stream()
                .map(k -> collection + ":" + k)
                .collect(Collectors.toSet());

        final DocIdOverlapLock.UniqueIdsPerRequest lockToken;
        if (lock != null) {
            lockToken = lock.lock(namespacedKeys);
        } else {
            // For the recovery scenario
            lockToken = null;
        }

        try {
            IndexResponse.Builder builder = IndexResponse.newBuilder();
            long maxSeqNo = -1;
            for (IndexRequestContext requestContext : requestContexts) {
                Status respStatus = null;
                switch (requestContext.item.getAction()) {
                    case UPDATE -> respStatus = handleUpdate(requestContext, indexSchema, indexWriter);
                    case DELETE ->
                            respStatus = handleDelete(requestContext.parsedDocument, indexSchema.getInitializedSchema(), indexWriter);
                    case UNRECOGNIZED ->
                            throw new IllegalArgumentException("Invalid Input for type index action detected: " + requestContext.item.getAction());
                }
                builder.addItemResponse(respStatus);
            }
            // Capture the max seqNo after all operations complete
            maxSeqNo = indexWriter.getMaxCompletedSequenceNumber();
            return new IndexResult(builder.build(), maxSeqNo);
        } finally {
            if (lock != null && lockToken != null) {
                lock.unlock(lockToken);
            }
        }

    }

    private static Status handleDelete(JsonNode parsedDocument, InitializedIndexSchema indexSchema, IndexWriter indexWriter) {
        try {
            String keyFieldName = indexSchema.getKeyFieldName();
            String text = parsedDocument.get(keyFieldName).asText();
            if (StringUtils.isEmpty(text)) {
                throw new IllegalArgumentException("Must specify value of key field and value of keyField");
            }

            indexWriter.deleteDocuments(new Term(keyFieldName, text));
            return Status.newBuilder().setCode(Code.OK.getNumber()).build();
        } catch (IllegalArgumentException e) {
            return Status.newBuilder().setCode(Code.INVALID_ARGUMENT.getNumber()).setMessage(e.getMessage()).build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete document", e);
        }
    }

    private static Status handleUpdate(IndexRequestContext context, IndexSchema indexSchema, IndexWriter indexWriter) {
        try {
            Document document = new Document();
            FieldMapperStrategy.addSourceField(document, context.item.getDocument());
            // Iterate over the fields
            InitializedIndexSchema initializedSchema = indexSchema.getInitializedSchema();
            Map<String, FieldMapperStrategy> strategyMap = initializedSchema.getFieldMapperStrategyMap().get();
            for (Iterator<Map.Entry<String, JsonNode>> it = context.parsedDocument.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = it.next();
                String fieldName = entry.getKey();
                JsonNode fieldValue = entry.getValue();
                FieldMapperStrategy strategy = strategyMap.get(fieldName);
                strategy.addFields(indexSchema, document, fieldName, fieldValue);
            }
            // Inject system fields only when TTL/memory tiers are enabled in schema
            if (indexSchema.isTtlEnabled()) {
                SystemFields.addSystemFields(document, context.item);
            }

            String keyFieldName = initializedSchema.getKeyFieldName();
            String text = context.parsedDocument.get(keyFieldName).asText();
            if (StringUtils.isEmpty(text)) {
                throw new IllegalArgumentException("Value of key field can not be blank");
            }
            indexWriter.updateDocument(new Term(keyFieldName, text), document);
            return Status.newBuilder().setCode(Code.OK.getNumber()).build();
        } catch (IllegalArgumentException e) {
            return Status.newBuilder().setCode(Code.INVALID_ARGUMENT.getNumber()).setMessage(e.getMessage()).build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to update document", e);
        }
    }

    private static List<IndexRequestContext> getRequestContexts(IndexRequest req) {
        return req.getItemList().stream().map((s) -> {
            try {
                return new IndexRequestContext(s, OBJECT_MAPPER.readTree(s.getDocument()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).toList();
    }

    private static Set<String> getUniqueDocKeysForRequest(List<IndexRequestContext> requestContexts,
                                                          InitializedIndexSchema indexSchema) {
        final String keyFieldName = indexSchema.getKeyFieldName();
        return requestContexts.stream().map(context -> context.parsedDocument.get(keyFieldName).asText()).collect(Collectors.toSet());
    }

    public record IndexRequestContext(IndexItem item, JsonNode parsedDocument) {

    }
}
