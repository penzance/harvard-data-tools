package edu.harvard.canvas_data.aws_data_tools.canvas;

import java.io.IOException;

import edu.harvard.canvas_data.aws_data_tools.VerificationException;

public interface Verifier {

  void verify() throws VerificationException, IOException;

}
