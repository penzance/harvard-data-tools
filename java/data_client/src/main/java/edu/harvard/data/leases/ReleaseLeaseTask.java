package edu.harvard.data.leases;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Simple wrapper application around a lease release operation, intended to be
 * called as a step in the data pipeline. The main method for this class will
 * release the named lease if it is currently owned by the expected owner.
 */
public class ReleaseLeaseTask {
  private static final Logger log = LogManager.getLogger();

  public static void main(final String[] args) throws InterruptedException {
    final String leaseTable = args[0];
    final String leaseName = args[1];
    final String owner = args[2];

    final LeaseManager mgr = new LeaseManager(leaseTable);
    mgr.release(leaseName, owner);
    log.info("Released lease " + leaseName + " in table " + leaseTable);
  }
}
