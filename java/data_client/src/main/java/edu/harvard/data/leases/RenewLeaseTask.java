package edu.harvard.data.leases;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Simple wrapper application around a lease renew operation, intended to be
 * called as a step in the data pipeline. The main method for this class will
 * attempt to renew the named lease on behalf of the given owner. If the owner
 * does not currently hold the lease, this application will terminate with a
 * non-zero status code.
 */
public class RenewLeaseTask {
  private static final Logger log = LogManager.getLogger();

  public static void main(final String[] args) throws InterruptedException, LeaseRenewalException {
    final String leaseTable = args[0];
    final String leaseName = args[1];
    final String owner = args[2];
    final int seconds = Integer.parseInt(args[3]);

    final LeaseManager mgr = new LeaseManager(leaseTable);
    final Lease lease = mgr.renew(leaseName, owner, seconds);
    log.info("Renewed lease " + leaseName + " for " + owner + " for " + lease.timeRemainingSeconds()
    + " more seconds");
  }

}
