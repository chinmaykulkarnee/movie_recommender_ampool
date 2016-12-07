package io.ampool;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Created by adongre on 10/17/16.
 */
public class FileFilter {
  public File[] finder(final String dirName, final String suffix) {
    File dir = new File(dirName);
    return dir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String filename) {
        return filename.endsWith(suffix);
      }
    });
  }
}
