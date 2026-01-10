package io.github.pretenderdb.cli.dagger;

import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import dagger.Module;
import dagger.Provides;
import io.github.pretenderdb.cli.exporter.CsvExporter;
import io.github.pretenderdb.cli.exporter.DynamoDbJsonExporter;
import io.github.pretenderdb.cli.exporter.IonExporter;
import io.github.pretenderdb.cli.exporter.TableExporter;
import io.github.pretenderdb.cli.importer.CsvImporter;
import io.github.pretenderdb.cli.importer.DynamoDbJsonImporter;
import io.github.pretenderdb.cli.importer.IonImporter;
import io.github.pretenderdb.cli.importer.TableImporter;
import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.manager.PdbTableManager;
import javax.inject.Singleton;

/**
 * Dagger module providing CLI dependencies.
 */
@Module
public class CliModule {

  /**
   * Provide Ion system.
   *
   * @return the ion system
   */
  @Provides
  @Singleton
  public IonSystem ionSystem() {
    return IonSystemBuilder.standard().build();
  }

  /**
   * Provide CSV exporter.
   *
   * @param objectMapper the object mapper
   * @param attributeValueConverter the attribute value converter
   * @return the CSV exporter
   */
  @Provides
  @Singleton
  public CsvExporter csvExporter(
      final ObjectMapper objectMapper, final AttributeValueConverter attributeValueConverter) {
    return new CsvExporter(objectMapper, attributeValueConverter);
  }

  /**
   * Provide DynamoDB JSON exporter.
   *
   * @param objectMapper the object mapper
   * @param attributeValueConverter the attribute value converter
   * @return the DynamoDB JSON exporter
   */
  @Provides
  @Singleton
  public DynamoDbJsonExporter dynamoDbJsonExporter(
      final ObjectMapper objectMapper, final AttributeValueConverter attributeValueConverter) {
    return new DynamoDbJsonExporter(objectMapper, attributeValueConverter);
  }

  /**
   * Provide Ion exporter.
   *
   * @param ionSystem the ion system
   * @return the Ion exporter
   */
  @Provides
  @Singleton
  public IonExporter ionExporter(final IonSystem ionSystem) {
    return new IonExporter(ionSystem);
  }

  /**
   * Provide CSV importer.
   *
   * @param objectMapper the object mapper
   * @param attributeValueConverter the attribute value converter
   * @return the CSV importer
   */
  @Provides
  @Singleton
  public CsvImporter csvImporter(
      final ObjectMapper objectMapper, final AttributeValueConverter attributeValueConverter) {
    return new CsvImporter(objectMapper, attributeValueConverter);
  }

  /**
   * Provide DynamoDB JSON importer.
   *
   * @param objectMapper the object mapper
   * @param attributeValueConverter the attribute value converter
   * @return the DynamoDB JSON importer
   */
  @Provides
  @Singleton
  public DynamoDbJsonImporter dynamoDbJsonImporter(
      final ObjectMapper objectMapper, final AttributeValueConverter attributeValueConverter) {
    return new DynamoDbJsonImporter(objectMapper, attributeValueConverter);
  }

  /**
   * Provide Ion importer.
   *
   * @param ionSystem the ion system
   * @return the Ion importer
   */
  @Provides
  @Singleton
  public IonImporter ionImporter(final IonSystem ionSystem) {
    return new IonImporter(ionSystem);
  }

  /**
   * Provide table exporter.
   *
   * @param pdbTableManager the table manager
   * @param pdbItemManager the item manager
   * @param pdbTableConverter the table converter
   * @param csvExporter the CSV exporter
   * @param dynamoDbJsonExporter the DynamoDB JSON exporter
   * @param ionExporter the Ion exporter
   * @param objectMapper the object mapper
   * @return the table exporter
   */
  @Provides
  @Singleton
  public TableExporter tableExporter(
      final PdbTableManager pdbTableManager,
      final PdbItemManager pdbItemManager,
      final PdbTableConverter pdbTableConverter,
      final CsvExporter csvExporter,
      final DynamoDbJsonExporter dynamoDbJsonExporter,
      final IonExporter ionExporter,
      final ObjectMapper objectMapper) {
    return new TableExporter(
        pdbTableManager,
        pdbItemManager,
        pdbTableConverter,
        csvExporter,
        dynamoDbJsonExporter,
        ionExporter,
        objectMapper);
  }

  /**
   * Provide table importer.
   *
   * @param pdbTableManager the table manager
   * @param pdbItemManager the item manager
   * @param pdbTableConverter the table converter
   * @param csvImporter the CSV importer
   * @param dynamoDbJsonImporter the DynamoDB JSON importer
   * @param ionImporter the Ion importer
   * @param objectMapper the object mapper
   * @return the table importer
   */
  @Provides
  @Singleton
  public TableImporter tableImporter(
      final PdbTableManager pdbTableManager,
      final PdbItemManager pdbItemManager,
      final PdbTableConverter pdbTableConverter,
      final CsvImporter csvImporter,
      final DynamoDbJsonImporter dynamoDbJsonImporter,
      final IonImporter ionImporter,
      final ObjectMapper objectMapper) {
    return new TableImporter(
        pdbTableManager,
        pdbItemManager,
        pdbTableConverter,
        csvImporter,
        dynamoDbJsonImporter,
        ionImporter,
        objectMapper);
  }
}
