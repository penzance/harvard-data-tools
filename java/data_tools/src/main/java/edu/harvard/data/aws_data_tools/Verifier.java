package edu.harvard.data.aws_data_tools;

import java.io.IOException;

public interface Verifier {

  void verify() throws VerificationException, IOException;

}
