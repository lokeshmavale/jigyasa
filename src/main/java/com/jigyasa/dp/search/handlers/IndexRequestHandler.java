package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.mappers.FieldMapperStrategy;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.protocol.IndexResponse;
import com.jigyasa.dp.search.services.RequestHandlerBase;
import com.jigyasa.dp.search.utils.DocIdOverlapLock;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
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
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final DocIdOverlapLock lock;
    private final HandlerHelpers handlerHelpers;

    public IndexRequestHandler(DocIdOverlapLock lock, HandlerHelpers handlerHelpers) {
        super("Index");
        this.lock = lock;
        this.handlerHelpers = handlerHelpers;
    }

    @Override
    public void internalHandle(IndexRequest req, StreamObserver<IndexResponse> observer) {

        IndexWriterManagerISCH indexWriterManager = null;
        try {
            indexWriterManager = this.handlerHelpers.getIndexWriterManager();
            final IndexWriter writer = indexWriterManager.acquireWriter();
            IndexSchema indexSchema = this.handlerHelpers.getIndexSchemaManager().getIndexSchema();
            IndexResponse indexResponse = processIndexRequests(req, indexSchema, writer, lock);
            this.handlerHelpers.getTranslogAppenderManager().getAppender().append(req);
            observer.onNext(indexResponse);
            observer.onCompleted();
        } catch (Exception e) {
            //Todo: Add better logging
            e.printStackTrace();
            observer.onError(e);
            observer.onCompleted();
        } finally {

            if (indexWriterManager != null) {
                indexWriterManager.releaseWriter();
            }
        }
    }

    public static IndexResponse processIndexRequests(IndexRequest req, IndexSchema indexSchema, IndexWriter indexWriter, DocIdOverlapLock lock) throws InterruptedException, TimeoutException {
        List<IndexRequestContext> requestContexts = getRequestContexts(req);
        Set<String> uniqueDocKeysForRequest = getUniqueDocKeysForRequest(requestContexts, indexSchema.getInitializedSchema());
        final DocIdOverlapLock.UniqueIdsPerRequest lockToken;
        if (lock != null) {
            lockToken = lock.lock(uniqueDocKeysForRequest);
        } else {
            // For the recovery scenario
            lockToken = null;
        }

        try {
            IndexResponse.Builder builder = IndexResponse.newBuilder();
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
            return builder.build();
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
            throw new RuntimeException("Failed to update document", e);
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
