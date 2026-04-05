package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.mappers.FieldMapperStrategy;
import com.jigyasa.dp.search.protocol.LookupRequest;
import com.jigyasa.dp.search.protocol.LookupResponse;
import com.jigyasa.dp.search.services.RequestHandlerBase;
import com.jigyasa.dp.search.utils.SourceVisitor;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

import java.nio.charset.StandardCharsets;

public class LookupRequestHandler extends RequestHandlerBase<LookupRequest, LookupResponse> {

    private static StoredFieldVisitor getVisitor() {
        return new StoredFieldVisitor() {
            @Getter
            byte[] src;

            @Override
            public void binaryField(FieldInfo fieldInfo, byte[] value) {
                this.src = value;
            }

            @Override
            public Status needsField(FieldInfo fieldInfo) {
                return FieldMapperStrategy.SOURCE_FIELD_NAME.equals(fieldInfo.name) ? Status.YES : Status.NO;
            }
        };
    }

    private final CollectionRegistry registry;

    public LookupRequestHandler(CollectionRegistry registry) {
        super("Lookup");
        this.registry = registry;
    }

    @Override
    public void internalHandle(LookupRequest req, StreamObserver<LookupResponse> observer) {
        HandlerHelpers helpers = registry.resolveHelpers(req.getCollection());
        try (var lease = helpers.getIndexSearcherManager().leaseSearcher()) {
            InitializedIndexSchema initializedSchema = helpers.getIndexSchemaManager().getIndexSchema().getInitializedSchema();
            String keyFieldName = initializedSchema.getKeyFieldName();
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.setMinimumNumberShouldMatch(1);
            for (String s : req.getDocKeysList()) {
                builder.add(new TermQuery(new Term(keyFieldName, s)), BooleanClause.Occur.SHOULD);
            }

            TopDocs search = lease.searcher().search(builder.build(), req.getDocKeysCount());

            LookupResponse.Builder response = LookupResponse.newBuilder();
            for (ScoreDoc scoreDoc : search.scoreDocs) {
                SourceVisitor visitor = new SourceVisitor();
                lease.searcher().storedFields().document(scoreDoc.doc, visitor);

                if (visitor.getSrc() != null) {
                    response.addDocuments(new String(visitor.getSrc(), StandardCharsets.UTF_8));
                }
            }

            observer.onNext(response.build());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }


}
