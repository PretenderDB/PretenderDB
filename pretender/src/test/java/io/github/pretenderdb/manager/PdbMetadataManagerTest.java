package io.github.pretenderdb.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

import io.github.pretenderdb.dao.PdbMetadataDao;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbMetadata;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PdbMetadataManagerTest {


  private static final PdbMetadata PDB_TABLE = ImmutablePdbMetadata.builder()
      .name("name")
      .hashKey("hashKey")
      .sortKey("sortKey")
      .createDate(Instant.now().truncatedTo(ChronoUnit.MILLIS))
      .build();

  @Mock private PdbMetadataDao dao;
  @Mock private PdbItemTableManager itemTableManager;
  @Mock private PdbStreamTableManager streamTableManager;
  @Mock private StatementContext context;

  @InjectMocks private PdbTableManager manager;

  @Test
  void testInsertPdbTable() {
    when(dao.insert(PDB_TABLE)).thenReturn(true);
    assertThat(manager.insertPdbTable(PDB_TABLE)).isTrue();
  }

  @Test
  void testInsertPdbTable_alreadyExists() {
    when(dao.getTable(PDB_TABLE.name())).thenReturn(of(PDB_TABLE));
    assertThat(manager.insertPdbTable(PDB_TABLE)).isFalse();
  }

  @Test
  void testInsertPdbTable_alreadyExistsInDbError() {
    when(dao.insert(PDB_TABLE)).thenThrow(new UnableToExecuteStatementException("Table already exists", new SQLIntegrityConstraintViolationException(), context));
    assertThat(manager.insertPdbTable(PDB_TABLE)).isFalse();
  }

  @Test
  void testInsertPdbTableError() {
    when(dao.insert(PDB_TABLE)).thenThrow(new UnableToExecuteStatementException("Unable to insert table", new RuntimeException(), context));
    assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> manager.insertPdbTable(PDB_TABLE));
  }

  @Test
  void testGetPdbTable() {
    when(dao.getTable(PDB_TABLE.name())).thenReturn(of(PDB_TABLE));
    assertThat(manager.getPdbTable(PDB_TABLE.name())).contains(PDB_TABLE);
  }

  @Test
  void testGetPdbTableNotFound() {
    when(dao.getTable(PDB_TABLE.name())).thenReturn(empty());
    assertThat(manager.getPdbTable(PDB_TABLE.name())).isEmpty();
  }

  @Test
  void testDeletePdbTable() {
    when(dao.delete(PDB_TABLE.name())).thenReturn(true);
    assertThat(manager.deletePdbTable(PDB_TABLE.name())).isTrue();
  }

  @Test
  void testDeletePdbTableNotFound() {
    when(dao.delete(PDB_TABLE.name())).thenReturn(false);
    assertThat(manager.deletePdbTable(PDB_TABLE.name())).isFalse();
  }

  @Test
  void testListPdbTables() {
    when(dao.listTableNames()).thenReturn(List.of(PDB_TABLE.name()));
    assertThat(manager.listPdbTables()).containsExactly(PDB_TABLE.name());
  }

  @Test
  void testEnableTtl() {
    final String ttlAttributeName = "expireAt";
    manager.enableTtl(PDB_TABLE.name(), ttlAttributeName);
    // Verify updateTtl was called with correct parameters
    org.mockito.Mockito.verify(dao).updateTtl(PDB_TABLE.name(), ttlAttributeName, true);
  }

  @Test
  void testDisableTtl() {
    manager.disableTtl(PDB_TABLE.name());
    // Verify updateTtl was called to disable TTL
    org.mockito.Mockito.verify(dao).updateTtl(PDB_TABLE.name(), null, false);
  }

  @Test
  void testEnableStream() {
    final String streamViewType = "NEW_AND_OLD_IMAGES";
    when(dao.getTable(PDB_TABLE.name())).thenReturn(of(PDB_TABLE));

    manager.enableStream(PDB_TABLE.name(), streamViewType);

    // Verify stream table was created
    org.mockito.Mockito.verify(streamTableManager).createStreamTable(PDB_TABLE.name());
    // Verify metadata was updated with stream config (ARN and label will be dynamic)
    org.mockito.Mockito.verify(dao).updateStreamConfig(
        org.mockito.Mockito.eq(PDB_TABLE.name()),
        org.mockito.Mockito.eq(true),
        org.mockito.Mockito.eq(streamViewType),
        org.mockito.Mockito.anyString(),  // streamArn is dynamically generated
        org.mockito.Mockito.anyString()   // streamLabel is dynamically generated
    );
  }

  @Test
  void testEnableStream_tableNotFound() {
    when(dao.getTable(PDB_TABLE.name())).thenReturn(empty());

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> manager.enableStream(PDB_TABLE.name(), "NEW_IMAGE"))
        .withMessage("Table not found: " + PDB_TABLE.name());
  }

  @Test
  void testDisableStream() {
    manager.disableStream(PDB_TABLE.name());
    // Verify metadata was updated to disable streams
    org.mockito.Mockito.verify(dao).updateStreamConfig(PDB_TABLE.name(), false, null, null, null);
  }

}