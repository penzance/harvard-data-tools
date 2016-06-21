package edu.harvard.data.leases;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AcquireLeaseTask {
  private static final Logger log = LogManager.getLogger();
  private static final int MAX_WAIT_SECONDS = 60;

  public static void main(final String[] args) throws InterruptedException {
    final String leaseTable = args[0];
    final String leaseName = args[1];
    final String owner = args[2];
    final int seconds = Integer.parseInt(args[3]);

    final LeaseManager mgr = new LeaseManager(leaseTable);
    Lease lease = mgr.acquire(leaseName, owner, seconds);
    while (lease == null) {
      int waitTime = mgr.getLeaseExpirationSeconds(leaseName);
      if (waitTime > MAX_WAIT_SECONDS) {
        waitTime = MAX_WAIT_SECONDS;
      }
      log.info("Waiting for " + waitTime + " seconds");
      Thread.sleep(waitTime * 1000);
      lease = mgr.acquire(leaseName, owner, seconds);
    }
    log.info("Acquired lease " + leaseName + " for " + owner + " for "
        + lease.timeRemainingSeconds() + " more seconds");
  }

}
