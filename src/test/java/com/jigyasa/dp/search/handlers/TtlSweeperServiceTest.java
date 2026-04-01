package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.models.IndexSchemaManager;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TtlSweeperServiceTest {

    private IndexWriterManagerISCH writerManager;
    private IndexSearcherManagerISCH searcherManager;
    private IndexSchemaManager schemaManager;
    private ScheduledExecutorService executor;
    private TtlSweeperService sweeper;

    @BeforeEach
    void setUp() {
        writerManager = mock(IndexWriterManagerISCH.class);
        searcherManager = mock(IndexSearcherManagerISCH.class);
        schemaManager = mock(IndexSchemaManager.class);
        executor = mock(ScheduledExecutorService.class);
        sweeper = new TtlSweeperService(writerManager, searcherManager, schemaManager, executor);
    }

    @Test
    void start_schedulesPeriodicTask() {
        sweeper.start();

        verify(executor).scheduleWithFixedDelay(
                any(Runnable.class), eq(30L), eq(30L), eq(TimeUnit.SECONDS));
    }

    @Test
    void shutdown_cancelsTaskAndStopsExecutor() throws InterruptedException {
        ScheduledFuture<?> future = mock(ScheduledFuture.class);
        doReturn(future).when(executor).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
        when(executor.awaitTermination(anyLong(), any())).thenReturn(true);

        sweeper.start();
        sweeper.shutdown();

        verify(future).cancel(false);
        verify(executor).shutdown();
        verify(executor).awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void shutdown_forcesIfNotTerminated() throws InterruptedException {
        ScheduledFuture<?> future = mock(ScheduledFuture.class);
        doReturn(future).when(executor).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
        when(executor.awaitTermination(anyLong(), any())).thenReturn(false);

        sweeper.start();
        sweeper.shutdown();

        verify(executor).shutdownNow();
    }
}
