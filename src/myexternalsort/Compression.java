
/* This interface provides the Compress and Deompress methods
 * using the Run-Length Encoding Compression
 */

package myexternalsort;

/**
 *
 * @author Nino
 */

public interface Compression {
    
    abstract String compressCSV(String csvFilePath, boolean containsHeader, boolean quoted);
    abstract void decompressCSV(String csvFilePath, boolean containsHeader, boolean quoted);
    
    
}
