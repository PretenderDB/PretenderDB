package io.github.pretenderdb;

import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.manager.PdbTableManager;
import io.github.pretenderdb.model.PdbMetadata;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * The type Dynamo db pretender client.
 */
@Singleton
public class DynamoDbPretenderClient implements DynamoDbClient {

  private static final Logger log = LoggerFactory.getLogger(DynamoDbPretenderClient.class);

  private static final String SERVICE_NAME = "dynamodb";
  private static final String TEST_STRING = "Ignore this please";
  private final PdbTableManager pdbTableManager;
  private final PdbTableConverter pdbTableConverter;
  private final PdbItemManager pdbItemManager;

  /**
   * Instantiates a new Dynamo db pretender client.
   *
   * @param pdbTableManager   the metadata manager
   * @param pdbTableConverter the pdb table converter
   * @param pdbItemManager    the item manager
   */
  @Inject
  public DynamoDbPretenderClient(final PdbTableManager pdbTableManager,
                                 final PdbTableConverter pdbTableConverter,
                                 final PdbItemManager pdbItemManager) {
    log.info("DynamoDbPretenderClient({},{},{})", pdbTableManager, pdbTableConverter, pdbItemManager);
    this.pdbTableManager = pdbTableManager;
    this.pdbTableConverter = pdbTableConverter;
    this.pdbItemManager = pdbItemManager;
  }

  @Override
  public String serviceName() {
    return SERVICE_NAME;
  }

  @Override
  public void close() {

  }

  @Override
  public ListTablesResponse listTables(final ListTablesRequest listTablesRequest) throws AwsServiceException, SdkClientException {
    return ListTablesResponse.builder().tableNames(pdbTableManager.listPdbTables()).build();
  }

  @Override
  public CreateTableResponse createTable(final CreateTableRequest createTableRequest) throws AwsServiceException, SdkClientException {
    final PdbMetadata table = pdbTableConverter.convert(createTableRequest);
    if (pdbTableManager.insertPdbTable(table)) {
      return CreateTableResponse.builder().tableDescription(pdbTableConverter.fromPdbTable(table)).build();
    } else {
      throw ResourceInUseException.builder().message("Table already exists").build();
    }
  }

  @Override
  public DeleteTableResponse deleteTable(final DeleteTableRequest deleteTableRequest) throws AwsServiceException, SdkClientException {
    final String tableName = deleteTableRequest.tableName();
    final Optional<PdbMetadata> pdbTable = pdbTableManager.getPdbTable(tableName);
    if (pdbTable.isEmpty()) {
      throw ResourceNotFoundException.builder().message("Table not found").build();
    }
    if (pdbTableManager.deletePdbTable(tableName)) {
      return DeleteTableResponse.builder().tableDescription(pdbTableConverter.fromPdbTable(pdbTable.get())).build();
    } else {
      throw InternalServerErrorException.builder().message("Unable to delete table").build();
    }
  }

  @Override
  public PutItemResponse putItem(final PutItemRequest putItemRequest) throws AwsServiceException, SdkClientException {
    return pdbItemManager.putItem(putItemRequest);
  }

  @Override
  public GetItemResponse getItem(final GetItemRequest getItemRequest) throws AwsServiceException, SdkClientException {
    return pdbItemManager.getItem(getItemRequest);
  }

  @Override
  public UpdateItemResponse updateItem(final UpdateItemRequest updateItemRequest) throws AwsServiceException, SdkClientException {
    return pdbItemManager.updateItem(updateItemRequest);
  }

  @Override
  public DeleteItemResponse deleteItem(final DeleteItemRequest deleteItemRequest) throws AwsServiceException, SdkClientException {
    return pdbItemManager.deleteItem(deleteItemRequest);
  }

  @Override
  public QueryResponse query(final QueryRequest queryRequest) throws AwsServiceException, SdkClientException {
    return pdbItemManager.query(queryRequest);
  }

  @Override
  public ScanResponse scan(final ScanRequest scanRequest) throws AwsServiceException, SdkClientException {
    return pdbItemManager.scan(scanRequest);
  }

  @Override
  public BatchGetItemResponse batchGetItem(final BatchGetItemRequest batchGetItemRequest) throws AwsServiceException, SdkClientException {
    return pdbItemManager.batchGetItem(batchGetItemRequest);
  }

  @Override
  public BatchWriteItemResponse batchWriteItem(final BatchWriteItemRequest batchWriteItemRequest) throws AwsServiceException, SdkClientException {
    return pdbItemManager.batchWriteItem(batchWriteItemRequest);
  }

  @Override
  public TransactGetItemsResponse transactGetItems(final TransactGetItemsRequest transactGetItemsRequest) throws AwsServiceException, SdkClientException {
    return pdbItemManager.transactGetItems(transactGetItemsRequest);
  }

  @Override
  public TransactWriteItemsResponse transactWriteItems(final TransactWriteItemsRequest transactWriteItemsRequest) throws AwsServiceException, SdkClientException {
    return pdbItemManager.transactWriteItems(transactWriteItemsRequest);
  }
}
