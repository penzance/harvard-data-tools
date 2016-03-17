package edu.harvard.data.data_tools;

import java.io.IOException;

import edu.harvard.data.client.VerificationException;

public interface Verifier {

  void verify() throws VerificationException, IOException;

}
