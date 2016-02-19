package edu.harvard.data.data_tools;

import java.io.IOException;

public interface Verifier {

  void verify() throws VerificationException, IOException;

}
