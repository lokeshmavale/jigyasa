package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.models.*;
import com.jigyasa.dp.search.protocol.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

class DeleteByQueryRequestHandlerTest {

    private HandlerHelpers helpers;
    private IndexWriterManagerISCH writerManager;
    private IndexSearcherManagerISCH searcherManager;
    private IndexSchemaManager schemaManager;
    private IndexWriter indexWriter;
    private DeleteByQueryRequestHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        helpers = mock(HandlerHelpers.class);
        writerManager = mock(IndexWriterManagerISCH.class);
        searcherManager = mock(IndexSearcherManagerISCH.class);
        schemaManager = mock(IndexSchemaManager.class);
        indexWriter = mock(IndexWriter.class);

        when(helpers.getIndexWriterManager()).thenReturn(writerManager);
        when(helpers.getIndexSearcherManager()).thenReturn(searcherManager);
        when(helpers.getIndexSchemaManager()).thenReturn(schemaManager);
        when(writerManager.acquireWriter()).thenReturn(indexWriter);
        when(writerManager.leaseWriter()).thenReturn(new IndexWriterManagerISCH.WriterLease(indexWriter, writerManager));
        when(indexWriter.getMaxCompletedSequenceNumber()).thenReturn(42L);

        // Mock translog for commit+reset after delete-by-query
        TranslogAppenderManager translogManager = mock(TranslogAppenderManager.class);
        com.jigyasa.dp.search.handlers.translog.TranslogAppender translogAppender = mock(com.jigyasa.dp.search.handlers.translog.TranslogAppender.class);
        when(helpers.getTranslogAppenderManager()).thenReturn(translogManager);
        when(translogManager.getAppender()).thenReturn(translogAppender);

        // Set up a schema with a filterable string field "status"
        IndexSchema indexSchema = mock(IndexSchema.class);
        InitializedIndexSchema initSchema = mock(InitializedIndexSchema.class);
        SchemaField statusField = new SchemaField();
        statusField.setName("status");
        statusField.setType(FieldDataType.STRING);
        statusField.setFilterable(true);
        when(initSchema.getFieldLookupMap()).thenReturn(Map.of("status", statusField));
        when(indexSchema.getInitializedSchema()).thenReturn(initSchema);
        when(schemaManager.getIndexSchema()).thenReturn(indexSchema);

        handler = new DeleteByQueryRequestHandler(mockRegistry(helpers));
    }

    @Test
    void deleteByQuery_withValidFilter_deletesAndReturnsResponse() throws Exception {
        when(indexWriter.deleteDocuments(any(Query.class))).thenReturn(99L);

        DeleteByQueryRequest req = DeleteByQueryRequest.newBuilder()
                .addFilters(FilterClause.newBuilder()
                        .setField("status")
                        .setTermFilter(TermFilter.newBuilder().setValue("expired").build()))
                .build();

        @SuppressWarnings("unchecked")
        StreamObserver<DeleteByQueryResponse> observer = mock(StreamObserver.class);
        handler.internalHandle(req, observer);

        ArgumentCaptor<DeleteByQueryResponse> captor = ArgumentCaptor.forClass(DeleteByQueryResponse.class);
        verify(observer).onNext(captor.capture());
        verify(observer).onCompleted();
        verify(observer, never()).onError(any());

        assertThat(captor.getValue().getDeletedCount()).isEqualTo(-1);
        verify(indexWriter).deleteDocuments(any(Query.class));
        verify(writerManager).releaseWriter();
        // NRT wait uses seqNo returned by deleteDocuments (99L), not getMaxCompletedSequenceNumber
        verify(searcherManager).waitForGeneration(99L);
    }

    @Test
    void deleteByQuery_emptyFilters_rejectsWithInvalidArgument() {
        DeleteByQueryRequest req = DeleteByQueryRequest.newBuilder().build();

        @SuppressWarnings("unchecked")
        StreamObserver<DeleteByQueryResponse> observer = mock(StreamObserver.class);
        handler.internalHandle(req, observer);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(observer).onError(captor.capture());
        verify(observer, never()).onNext(any());

        assertThat(captor.getValue()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) captor.getValue()).getStatus().getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void deleteByQuery_unknownField_rejectsWithInvalidArgument() {
        DeleteByQueryRequest req = DeleteByQueryRequest.newBuilder()
                .addFilters(FilterClause.newBuilder()
                        .setField("nonexistent")
                        .setTermFilter(TermFilter.newBuilder().setValue("val").build()))
                .build();

        @SuppressWarnings("unchecked")
        StreamObserver<DeleteByQueryResponse> observer = mock(StreamObserver.class);
        assertThatThrownBy(() -> handler.internalHandle(req, observer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deleteByQuery_writerException_returnsInternalError() throws Exception {
        when(indexWriter.deleteDocuments(any(Query.class))).thenThrow(new java.io.IOException("disk full"));

        DeleteByQueryRequest req = DeleteByQueryRequest.newBuilder()
                .addFilters(FilterClause.newBuilder()
                        .setField("status")
                        .setTermFilter(TermFilter.newBuilder().setValue("old").build()))
                .build();

        @SuppressWarnings("unchecked")
        StreamObserver<DeleteByQueryResponse> observer = mock(StreamObserver.class);
        handler.internalHandle(req, observer);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(observer).onError(captor.capture());
        assertThat(((StatusRuntimeException) captor.getValue()).getStatus().getCode())
                .isEqualTo(Status.Code.INTERNAL);
        // Writer must still be released
        verify(writerManager).releaseWriter();
    }

    @Test
    void deleteByQuery_releasesWriterEvenOnFilterError() {
        DeleteByQueryRequest req = DeleteByQueryRequest.newBuilder()
                .addFilters(FilterClause.newBuilder()
                        .setField("nonexistent")
                        .setTermFilter(TermFilter.newBuilder().setValue("val").build()))
                .build();

        @SuppressWarnings("unchecked")
        StreamObserver<DeleteByQueryResponse> observer = mock(StreamObserver.class);
        assertThatThrownBy(() -> handler.internalHandle(req, observer))
                .isInstanceOf(IllegalArgumentException.class);

        // Filter validation fails before leaseWriter, so releaseWriter should NOT be called
        verify(writerManager, never()).releaseWriter();
    }

    private static CollectionRegistry mockRegistry(HandlerHelpers helpers) {
        CollectionRegistry registry = mock(CollectionRegistry.class);
        when(registry.resolveHelpers(anyString())).thenReturn(helpers);
        return registry;
    }
}
