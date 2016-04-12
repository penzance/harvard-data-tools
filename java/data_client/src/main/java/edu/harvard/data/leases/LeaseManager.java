package edu.harvard.data.leases;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.Builder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.ConsistentReads;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;

public class LeaseManager {

  private static final Logger log = LogManager.getLogger();
  private static DynamoDBMapper mapper;
  private static String tableName;
  private static DynamoDBMapperConfig mapperConfig;

  public static void init(final String table) {
    mapper = new DynamoDBMapper(new AmazonDynamoDBClient());
    tableName = table;
    final Builder builder = new DynamoDBMapperConfig.Builder();
    builder.setTableNameOverride(new TableNameOverride(tableName));
    builder.setConsistentReads(ConsistentReads.CONSISTENT);
    mapperConfig = builder.build();
  }

  public static Lease acquire(final String name, final String owner, final int seconds) {
    if (tableName == null) {
      throw new RuntimeException("Lease class used before init method called");
    }
    final Lease lease = find(name);
    try {
      log.info("Sleeping");
      Thread.sleep(15000);
      log.info("Slept");
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
    if (lease == null) {
      create(name, owner, seconds);
    } else if (lease.getOwner().equals(owner) || lease.timeRemainingSeconds() < 0) {
      lease.setTimeRemainingSeconds(seconds);
      lease.setOwner(owner);
      update(lease);
    }
    final Lease updatedLease = find(name);
    if (updatedLease.getOwner().equals(owner)) {
      return updatedLease;
    }
    return null;
  }

  public static int getLeaseExpirationSeconds(final String name) {
    final Lease lease = find(name);
    return lease.timeRemainingSeconds();
  }

  private static Lease find(final String id) {
    log.debug("Finding lease ID " + id + " from table " + tableName);
    return mapper.load(Lease.class, id, mapperConfig);
  }

  private static boolean create(final String name, final String owner, final int seconds) {
    final Lease lease = new Lease(name, owner, seconds, 0L);
    final DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
    final Map<String, ExpectedAttributeValue> expectations = new HashMap<String, ExpectedAttributeValue>();
    expectations.put("name", new ExpectedAttributeValue(false));
    saveExpression.setExpected(expectations);
    try {
      log.info("Creating lease: " + lease);
      mapper.save(lease, saveExpression, mapperConfig);
    } catch (final ConditionalCheckFailedException e) {
      // Somebody else created the row first. We'll detect this in acquire when
      // we see that the updatedLease does not belong to the owner.
      log.info("Failed to create lease " + lease + ". Key already exists in table");
      return false;
    }
    return true;
  }

  private static boolean update(final Lease lease) {
    log.info("Updating lease: " + lease);
    final String expectedVersion = Long.toString(lease.getVersion());
    lease.setVersion(lease.getVersion() + 1);

    final DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
    final Map<String, ExpectedAttributeValue> expectations = new HashMap<String, ExpectedAttributeValue>();
    expectations.put("version", new ExpectedAttributeValue(new AttributeValue().withN(expectedVersion)).withExists(true));
    saveExpression.setExpected(expectations);
    try {
      mapper.save(lease, saveExpression, mapperConfig);
    } catch (final ConditionalCheckFailedException e) {
      // Somebody else updated the row first. We'll detect this in acquire when
      // we see that the updatedLease does not belong to the owner.
      log.info("Failed to update " + lease + ". Expected version number " + expectedVersion);
      return false;
    }
    return true;
  }

}
