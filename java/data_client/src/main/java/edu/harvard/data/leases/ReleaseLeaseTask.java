package edu.harvard.data.leases;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
