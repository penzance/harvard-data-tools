package edu.harvard.data.leases;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import edu.harvard.data.FormatLibrary;

@DynamoDBTable(tableName = "DummyTableName")
public class Lease {

  @DynamoDBHashKey(attributeName = "name")
  private String name;

  @DynamoDBAttribute(attributeName = "owner")
  private String owner;

  @DynamoDBAttribute(attributeName = "expires")
  private String expires;

  @DynamoDBAttribute(attributeName = "version")
  private Long version;

  private static final DateFormat DATE_FORMAT = FormatLibrary.JSON_DATE_FORMAT;

  Lease(final String name, final String owner, final int seconds, final long version) {
    this.name = name;
    this.owner = owner;
    this.version = version;
    this.setTimeRemainingSeconds(seconds);
  }

  public Lease() {
  }

  public int timeRemainingSeconds() {
    if (expires == null) {
      return 0;
    }
    try {
      return (int) (DATE_FORMAT.parse(expires).getTime() - new Date().getTime() / 1000L);
    } catch (final ParseException e) {
      throw new RuntimeException(e);
    }
  }

  void setTimeRemainingSeconds(final int seconds) {
    final long expiration = new Date().getTime() + (seconds * 1000);
    this.expires = DATE_FORMAT.format(new Date(expiration));
  }

  //  public int renew(final int seconds) throws LeaseRenewalException {
  //    final Lease lease = LeaseManager.acquire(name, owner, seconds);
  //    if (lease == null) {
  //      throw new LeaseRenewalException();
  //    }
  //  }

  public void setName(final String name) {
    this.name = name;
  }

  public void setOwner(final String owner) {
    this.owner = owner;
  }

  public void setExpires(final String expires) {
    this.expires = expires;
  }

  public void setVersion(final Long version) {
    this.version = version;
  }

  public String getName() {
    return name;
  }

  public String getOwner() {
    return owner;
  }

  public String getExpires() {
    return expires;
  }

  public Long getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "Lease ID: " + name + " owned by " + owner + ". Expires: " + expires + ", version: "
        + version;
  }

}
