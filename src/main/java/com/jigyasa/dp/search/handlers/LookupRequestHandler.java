package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.protocol.LookupRequest;
import com.jigyasa.dp.search.protocol.LookupResponse;
import com.jigyasa.dp.search.services.RequestHandlerBase;
import com.jigyasa.dp.search.utils.SourceVisitor;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class LookupRequestHandler extends RequestHandlerBase<LookupRequest, LookupResponse> {

    private final CollectionRegistry registry;

    public LookupRequestHandler(CollectionRegistry registry) {
        super("Lookup");
        this.registry = registry;
    }

    @Override
    public void internalHandle(LookupRequest req, StreamObserver<LookupResponse> observer) {
        HandlerHelpers helpers = registry.resolveHelpers(req.getCollection());
        try (var lease = helpers.indexSearcherManager().leaseSearcher()) {
            InitializedIndexSchema initializedSchema = helpers.indexSchemaManager().getIndexSchema().getInitializedSchema();
            String keyFieldName = initializedSchema.getKeyFieldName();
            int keyCount = req.getDocKeysCount();

            // Use TermInSetQuery for batch lookups (O(1) per segment via automaton),
            // single TermQuery for single-key lookups.
            Query query;
            if (keyCount == 1) {
                query = new TermQuery(new Term(keyFieldName, req.getDocKeys(0)));
            } else {
                List<BytesRef> terms = new ArrayList<>(keyCount);
                for (String key : req.getDocKeysList()) {
                    terms.add(new BytesRef(key));
                }
                query = new TermInSetQuery(keyFieldName, terms);
            }

            TopDocs search = lease.searcher().search(query, keyCount);

            LookupResponse.Builder response = LookupResponse.newBuilder();
            var storedFields = lease.searcher().storedFields();
            SourceVisitor visitor = new SourceVisitor();
            for (ScoreDoc scoreDoc : search.scoreDocs) {
                visitor.reset();
                storedFields.document(scoreDoc.doc, visitor);
                if (visitor.getSrc() != null) {
                    response.addDocuments(new String(visitor.getSrc(), StandardCharsets.UTF_8));
                }
            }

            observer.onNext(response.build());
            observer.onCompleted();
        } catch (IOException e) {
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }
}
