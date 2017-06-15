package edu.harvard.data.leases;

/**
 * This class contains an optional background thread that an application may run
 * to periodically update a lease that is owned by the process.
 *
 * The thread should be set up early in the application's run, in order to avoid
 * any currently-held lease expiring before the first renewal check. The
 * checkLease method should be called at the end of application to ensure that
 * there was no point during the life of the thread at which the lease was
 * unavailable to refresh.
 */
public class LeaseRenewalThread extends Thread {

  private final LeaseManager manager;
  private final Lease lease;
  private final int leaseLengthSeconds;
  private Throwable error;

  /**
   * Create and start a new LeaseRenewalThread with the provided settings.
   * Before calling this method, the process should ensure that the expected
   * owner currently holds the lease (otherwise the first renewal attempt will
   * fail).
   *
   * @param leaseTable
   *          the DynamoDB table that holds lease records. The same table must
   *          be used by all processes that intend to coordinate using a
   *          particular lease.
   * @param leaseName
   *          the globally-unique lease name. The same name must be used by all
   *          processes that intend to coordinate using a particular lease.
   * @param leaseOwner
   *          the expected owner of the lease. The thread will ensure that at no
   *          point during the run of the application does the lease belong to
   *          any owner other than this one.
   * @param leaseSeconds
   *          the frequency at which to refresh the lease, specified in seconds.
   * @return a new LeaseRenewalThread object that has already been started.
   */
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

  /**
   * Manually check whether the lease is currently owned by the expected owner.
   * If the lease cannot be renewed, or if at some point in the past it could
   * not be renewed, this method will throw an exception.
   *
   * @throws LeaseRenewalException
   *           if the lease currently belongs to some other process, or if it
   *           has ever belonged to another process since the thread started.
   */
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
