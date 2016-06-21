package edu.harvard.data.leases;

public class LeaseRenewalThread extends Thread {

  private final LeaseManager manager;
  private final Lease lease;
  private final int leaseLengthSeconds;
  private Throwable error;

  public static LeaseRenewalThread setup(final String leaseTable, final String leaseName,
      final String leaseOwner, final int leaseSeconds) {
    final LeaseManager manager = new LeaseManager(leaseTable);
    final Lease lease = manager.acquire(leaseName, leaseOwner, leaseSeconds);
    final LeaseRenewalThread thread = new LeaseRenewalThread(manager, lease, leaseSeconds);
    thread.start();
    return thread;
  }

  public LeaseRenewalThread(final LeaseManager manager, final Lease lease,
      final int leaseLengthSeconds) {
    this.lease = lease;
    this.manager = manager;
    this.leaseLengthSeconds = leaseLengthSeconds;
    this.setDaemon(true);
  }

  public void checkLease() throws LeaseRenewalException {
    if (error != null) {
      throw new LeaseRenewalException(error);
    }
    manager.renew(lease.getName(), lease.getOwner(), leaseLengthSeconds);
  }

  @Override
  public void run() {
    while (true) {
      try {
        checkLease();
        Thread.sleep(leaseLengthSeconds * 500);
      } catch (final LeaseRenewalException | InterruptedException e) {
        error = e;
        return;
      }
    }
  }

}
