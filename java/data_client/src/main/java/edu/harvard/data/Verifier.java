package edu.harvard.data;

import java.io.IOException;

public interface Verifier {

  void verify() throws VerificationException, IOException, DataConfigurationException;

}
