package edu.harvard.data.leases;

public class LeaseRenewalException extends Exception {

  private static final long serialVersionUID = 1L;

  public LeaseRenewalException(final Throwable cause) {
    super(cause);
  }

  public LeaseRenewalException(final String msg) {
    super(msg);
  }

}
