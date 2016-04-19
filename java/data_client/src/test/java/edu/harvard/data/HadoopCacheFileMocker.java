package edu.harvard.data;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.powermock.api.mockito.PowerMockito;

import edu.harvard.data.identity.IdentityReducerTests;

public class HadoopCacheFileMocker {

  public static FileSystem setupFilesystem(final Configuration config) throws IOException {
    final FileSystem fs = mock(FileSystem.class);
    PowerMockito.mockStatic(FileSystem.class);
    when(FileSystem.get(config)).thenReturn(fs);
    when(fs.exists(any(Path.class))).thenReturn(true);
    when(fs.isDirectory(any(Path.class))).thenReturn(false);
    return fs;
  }

  public static void mockInputStream(final FileSystem fs, final URI uri, final String fileName)
      throws IOException {
    final InputStream in = IdentityReducerTests.class.getClassLoader()
        .getResourceAsStream(fileName);
    final SeekableInputStream seekable = new SeekableInputStream(in);
    when(fs.open(new Path(uri))).thenReturn(new FSDataInputStream(seekable));
  }

}

class SeekableInputStream extends InputStream implements Seekable, PositionedReadable {

  private final InputStream in;

  public SeekableInputStream(final InputStream in) {
    this.in = in;
  }

  @Override
  public void seek(final long pos) throws IOException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public long getPos() throws IOException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public boolean seekToNewSource(final long targetPos) throws IOException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public int read(final long position, final byte[] buffer, final int offset, final int length)
      throws IOException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void readFully(final long position, final byte[] buffer, final int offset,
      final int length) throws IOException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void readFully(final long position, final byte[] buffer) throws IOException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public int read() throws IOException {
    return in.read();
  }

}