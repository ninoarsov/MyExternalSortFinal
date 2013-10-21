
/* This is a class that contains some useful values used throughout the
 * whole process of compression, as well as decompression.
 * It stores the OS, user name, OS-specific line separator and file separator.
 */
package myexternalsort;

/**
 *
 * @author nino
 */

public final class OS_Properties {
    
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");
    public static final String FILE_SEPARATOR = System.getProperty("file.separator");
    public static final String OS_NAME = System.getProperty("os.name");
    public static final String USER_NAME = System.getProperty("user.name");
    public static final String USER_HOME = System.getProperty("user.home");
    public static final String TEMP_DIR_PATH = System.getProperty("java.io.tmpdir");
    public static final int OS_LS_INDEX = (OS_NAME.contains("Windows")) ? 1 : 0;

}
