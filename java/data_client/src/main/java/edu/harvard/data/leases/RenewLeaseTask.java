package edu.harvard.data.leases;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RenewLeaseTask {
  private static final Logger log = LogManager.getLogger();

  public static void main(final String[] args) throws InterruptedException, LeaseRenewalException {
    final String leaseTable = args[0];
    final String leaseName = args[1];
    final String owner = args[2];
    final int seconds = Integer.parseInt(args[3]);

    final LeaseManager mgr = new LeaseManager(leaseTable);
    final Lease lease = mgr.renew(leaseName, owner, seconds);
    log.info("Renewed lease " + leaseName + " for " + owner + " for "
        + lease.timeRemainingSeconds() + " more seconds");
  }

}
