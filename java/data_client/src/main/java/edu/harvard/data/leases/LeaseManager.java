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
  private final DynamoDBMapper mapper;
  private final String tableName;
  private final DynamoDBMapperConfig mapperConfig;

  public LeaseManager(final String tableName) {
    this.tableName = tableName;
    mapper = new DynamoDBMapper(new AmazonDynamoDBClient());
    final Builder builder = new DynamoDBMapperConfig.Builder();
    builder.setTableNameOverride(new TableNameOverride(tableName));
    builder.setConsistentReads(ConsistentReads.CONSISTENT);
    mapperConfig = builder.build();
  }

  public Lease renew(final String name, final String owner, final int seconds)
      throws LeaseRenewalException {
    final Lease lease = find(name);
    if (lease.getOwner() == null || !lease.getOwner().equals(owner)) {
      // The owner doesn't currently hold the lease, so we can't renew.
      throw new LeaseRenewalException(
          "Failed to renew " + lease + ". " + owner + " does not currently hold the lease");
    }

    // We know that we own the lease as of when it was fetched from dynamo,
    // although this may have changed since then. We re-acquire the lease, then
    // check that there haven't been any updates between then and now.
    final Lease newLease = acquire(lease.getName(), lease.getOwner(), seconds);

    if (newLease == null) {
      // We couldn't acquire the lease; somebody else must have grabbed it.
      throw new LeaseRenewalException("Failed to renew " + lease);
    }
    if (newLease.getVersion() != lease.getVersion() + 1) {
      // Every lease update increments the version number, and we have just
      // updated the lease. If the version number has been updated by more than
      // one then there must have been some change to the state of the table
      // since we checked the lease (such as the lease expiring, and somebody
      // else acquiring it).
      throw new LeaseRenewalException("Failed to renew " + lease);
    }
    return newLease;
  }

  public Lease acquire(final String name, final String owner, final int seconds) {
    // Lookup the lease in the dynamo table.
    final Lease lease = find(name);

    if (lease == null) {
      // The lease does not exist in the table. Create it now. It is possible
      // that some other process will simultaneously create a lease with the
      // same name, so we can't assume that because we called create we will
      // automatically hold the newly-created lease; all this call ensures is
      // that a lease exists.
      create(name, owner, seconds);
    } else if (lease.getOwner() == null || lease.getOwner().equals(owner)
        || lease.timeRemainingSeconds() <= 0) {
      // The lease exists in the dynamo table, and either we currently hold it,
      // or nobody currently holds it (because it has expired). We will now try
      // to claim the lease, understanding that if somebody else has updated the
      // lease since we fetched it, our update operation will fail.
      lease.setTimeRemainingSeconds(seconds);
      lease.setOwner(owner);
      update(lease);
    } else {
      // The lease exists in the dynamo table, but is owned by somebody else. We
      // won't try to claim it.
      log.info("Failed to acquire lease " + lease + ". Owned by " + lease.getOwner()
      + " for another " + lease.timeRemainingSeconds() + " seconds");
    }

    // We now know that the lease exists in the dynamo table, and we have
    // attempted to claim it if it was available. We now fetch the lease again
    // to get the latest version
    final Lease updatedLease = find(name);

    // There are two possible cases here. Either we succeeded in claiming the
    // lease (in which case we are the owner), or we did not (in which case
    // somebody else is the owner). In the former case, we are guaranteed that
    // nobody else will update the dynamo table until the lease time runs out
    // (because of the conditional above), and that anybody who was racing with
    // us to update an expired lease will fail (because of the atomic update
    // operation).
    if (updatedLease.getOwner().equals(owner)) {
      // We successfully set the owner to be this process, and can return the
      // lease object.
      return updatedLease;
    } else {
      // Somebody else owns the lease.
      return null;
    }
  }

  public void release(final String name, final String owner) {
    // Lookup the lease in the dynamo table.
    final Lease lease = find(name);

    // If the lease doesn't exist, or has expired we don't need to do anything
    if (lease != null && lease.timeRemainingSeconds() > 0) {
      // If we don't own the lease, we can't release it
      if (owner.equals(lease.getOwner())) {
        lease.setOwner(null);
        update(lease);
      }
    }
    // We don't need to check the result; either we released the lease (which
    // was our intention), or the lease expired while we were working on it and
    // somebody else acquired it (which achieves the same goal).
  }

  public int getLeaseExpirationSeconds(final String name) {
    final Lease lease = find(name);
    return lease.timeRemainingSeconds();
  }

  // Load the lease from the dynamo table. Returns null if the lease is not
  // present.
  private Lease find(final String id) {
    log.debug("Finding lease ID " + id + " from table " + tableName);
    return mapper.load(Lease.class, id, mapperConfig);
  }

  private boolean create(final String name, final String owner, final int seconds) {
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

  // This method assumes that the caller has already guaranteed that the lease
  // exists, and that the current process has the right to modify the lease.
  //
  // Every lease has a version number that is incremented on every update. We
  // know what version number the lease previously had, so we can do the
  // Dynamo version of an atomic CAS operation. If the lease still has the
  // same version number we can safely update it. If not, the value of the
  // lease has changed since we last looked, so the update should fail.
  private void update(final Lease lease) {
    log.info("Updating lease: " + lease);

    // Store the previous value of the version number.
    final String expectedVersion = Long.toString(lease.getVersion());
    // Update the lease object to hold a new version number.
    lease.setVersion(lease.getVersion() + 1);

    // Setup an atomic operation that will succeed only if the version number
    // currently in dynamo matches the one that we stored above.
    final DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
    final Map<String, ExpectedAttributeValue> expectations = new HashMap<String, ExpectedAttributeValue>();
    expectations.put("version",
        new ExpectedAttributeValue(new AttributeValue().withN(expectedVersion)).withExists(true));
    saveExpression.setExpected(expectations);

    try {
      // Attempt to save the updated lease. This will throw an exception if the
      // lease is not updated.
      mapper.save(lease, saveExpression, mapperConfig);
    } catch (final ConditionalCheckFailedException e) {
      // Somebody else updated the row first. We'll detect this in acquire when
      // we see that the updatedLease does not belong to the owner.
      log.info("Failed to update " + lease + ". Expected version number " + expectedVersion);
    }
  }

}
