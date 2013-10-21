
/* This class implements a indexing unit which indexes every cell at its
 * beginning. It provides a crucial time performance boost to the compression 
 * procedure
 */
package myexternalsort;

import externallist.ExternalList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 *
 * @author Nino
 */
public class IndexingUnit {

    private String filePath;
    private RandomAccessFile raf;
    private FileChannel fc;
    private int rows;
    private int columns;

    public IndexingUnit(String filePath, int rows, int columns) throws FileNotFoundException {

        this.filePath = filePath;
        this.raf = new RandomAccessFile(this.filePath, "rw");
        this.fc = this.raf.getChannel();
        this.rows = rows;
        this.columns = columns;

    }

    /*This method generates a Binary Indexed Tree containing the positions of
     * each first byte in a cell
     */
    public FenwickTreeFile getIndexedCommas() throws IOException, Exception {

        long byteCounter = 0, bufferSize = 100 * 1024 * 1024, fileSize = Files.size(Paths.get(filePath));
        int k = 0;
        byte n;
        int commaOffset = 0;

        FenwickTreeFile f = new FenwickTreeFile(OS_Properties.TEMP_DIR_PATH + "fwt.dat", rows * columns);
        MappedByteBuffer IO = fc.map(FileChannel.MapMode.READ_ONLY, 0, fileSize < bufferSize ? fileSize : bufferSize);

        try {

            f.add(k, 0);
            k++;

            for (long i = 0; i < fileSize; i++) {
                if (!IO.hasRemaining()) {
                    IO = fc.map(FileChannel.MapMode.READ_ONLY, i, (fileSize - byteCounter) < bufferSize ? (fileSize - byteCounter) : bufferSize);
                }

                n = IO.get();

                byteCounter++;
                if ((char) n != OS_Properties.LINE_SEPARATOR.charAt(OS_Properties.OS_LS_INDEX) && (char) n != ',') {
                    commaOffset++;
                } else {
                    f.add(k, commaOffset + 1);
                    k++;
                    commaOffset = 0;
                }
            }
        } finally {
            fc.close();
            raf.close();
        }

        f.initRandomAccessFile();   //prepare the tree for reading

        return f;
    }

    /* This method puts an index at the beginning of each single-sorted 
     * column interval
     */
    public HashMap<Integer, ExternalList> indexColumns(String filePath, int[] order, FenwickTreeFile f) throws FileNotFoundException, IOException, Exception {

        //first one in order remains the same(null) as it is fully sorted
        HashMap<Integer, ExternalList> hashMap = new HashMap<>();
        RandomAccessFile localRaf = new RandomAccessFile(filePath, "rw");
        FileChannel fileChannel = localRaf.getChannel();
        long fileSize = Files.size(Paths.get(filePath));
        long bufStart = 0;
        long bufSize = fileSize < 100 * 1024 * 1024 ? fileSize : 100 * 1024 * 1024;
        MappedByteBuffer IO = fileChannel.map(FileChannel.MapMode.READ_WRITE, bufStart, bufSize);
        byte n;

        try {
            //no change to first column in order, it's fully sorted
            //the lists in the hashmap contain the indices of the Fenwick Tree to be read from
            hashMap.put(order[0], null);
            int pos = 0, index;
            for (int i = 1; i < order.length; i++) {
                String prev = null, curr;
                ExternalList l = new ExternalList();
                StringBuilder sb = new StringBuilder();

                l.add(order[i]);
                sb.setLength(0);

                for (int k = 0; k < i; k++) {
                    index = (int) f.read(order[k]);

                    if (index < bufStart || index + 128 > bufStart + bufSize) {
                        bufStart = index;
                        bufSize = (fileSize - index) < 100 * 1024 * 1024 ? (fileSize - index) : 100 * 1024 * 1024;
                        IO = fileChannel.map(FileChannel.MapMode.READ_ONLY, bufStart, bufSize);
                    }

                    n = IO.get(index - (int) bufStart);
                    index++;

                    while (!OS_Properties.LINE_SEPARATOR.contains(String.valueOf((char) n)) && (char) n != ',' && n != -1) {
                        sb.append((char) n);
                        n = IO.get(index - (int) bufStart);
                        index++;
                    }

                }
                prev = sb.toString();

                int row = 1;

                for (int k = order[i] + columns; k <= order[i] + (rows - 1) * columns; k += columns, row++) {
                    sb.setLength(0);
                    for (int c = 0; c < i; c++) {
                        pos = (int) f.read(order[c] + (row) * columns);
                        if (pos < bufStart || pos + 128 > bufStart + bufSize) {
                            bufStart = pos;
                            bufSize = (fileSize - pos) < 100 * 1024 * 1024 ? (fileSize - pos) : 100 * 1024 * 1024;
                            IO = fileChannel.map(FileChannel.MapMode.READ_ONLY, bufStart, bufSize);
                        }
                        n = IO.get(pos - (int) bufStart);
                        pos++;
                        while (!OS_Properties.LINE_SEPARATOR.contains(String.valueOf((char) n)) && (char) n != ',') {
                            sb.append((char) n);
                            n = IO.get(pos - (int) bufStart);
                            pos++;
                        }
                    }

                    curr = sb.toString();
                    if (!curr.equals(prev)) {
                        l.add(k);
                    }
                    prev = curr;
                    curr = null;
                }

                hashMap.put(order[i], l);

            }

            return hashMap;
        } finally {
            localRaf.close();
        }
    }
}
