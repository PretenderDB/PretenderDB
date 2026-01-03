package io.github.pretenderdb.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.github.pretenderdb.BaseJdbiTest;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbMetadata;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PdbMetadataDaoTest extends BaseJdbiTest {

  private static final PdbMetadata PDB_TABLE = ImmutablePdbMetadata.builder()
      .name("name")
      .hashKey("hashKey")
      .sortKey("sortKey")
      .createDate(Instant.now().truncatedTo(ChronoUnit.MILLIS))
      .build();

  private PdbMetadataDao pdbMetadataDao;

  @BeforeEach
  void setup() {
    pdbMetadataDao = jdbi.onDemand(PdbMetadataDao.class);
  }

  @Test
  void testRoundTrip() {
    assertThat(pdbMetadataDao.listTableNames()).isEmpty();
    assertThat(pdbMetadataDao.getTable(PDB_TABLE.name())).isEmpty();
    assertThat(pdbMetadataDao.insert(PDB_TABLE)).isEqualTo(true);
    assertThatExceptionOfType(UnableToExecuteStatementException.class)
        .isThrownBy(() -> pdbMetadataDao.insert(PDB_TABLE))
        .withCauseInstanceOf(SQLIntegrityConstraintViolationException.class);
    assertThat(pdbMetadataDao.listTableNames()).containsExactly(PDB_TABLE.name());
    assertThat(pdbMetadataDao.getTable(PDB_TABLE.name())).contains(PDB_TABLE);
    assertThat(pdbMetadataDao.delete(PDB_TABLE.name())).isEqualTo(true);
    assertThat(pdbMetadataDao.delete(PDB_TABLE.name())).isEqualTo(false);
    assertThat(pdbMetadataDao.listTableNames()).isEmpty();
    assertThat(pdbMetadataDao.getTable(PDB_TABLE.name())).isEmpty();
  }

}