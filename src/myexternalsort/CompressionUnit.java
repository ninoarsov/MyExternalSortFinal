
/* This class implements the Compression interface */
package myexternalsort;

import externallist.ExternalList;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Nino
 */
public class CompressionUnit implements Compression {

    private String csvFilePath;
    private String fenwickTreeFilePath;
    private int rows;
    private int columns;
    private int[] columnOrdering;
    private HashMap<Integer, Integer> distinctValuesByColumn;
    private HashMap<Integer, ArrayList<Integer>> columnLimitIndexing;
    private FenwickTreeFile csvIndexing;
    private RandomAccessFile csvRaf;

    public CompressionUnit() {

        this.csvFilePath = null;
        this.fenwickTreeFilePath = null;
        this.rows = 0;
        this.columns = 0;
        this.columnOrdering = null;
        this.distinctValuesByColumn = null;
        this.columnLimitIndexing = null;
        this.csvIndexing = null;

    }

    public CompressionUnit(String csvFilePath, String fenwickTreeFilePath) throws FileNotFoundException {

        this.csvFilePath = csvFilePath;
        this.fenwickTreeFilePath = fenwickTreeFilePath;

        try {
            this.csvRaf = new RandomAccessFile(this.csvFilePath, "rw");
        } catch (FileNotFoundException e) {
            System.out.println("File does not exist!");
            System.exit(1);
        }

    }

    public String getCsvFilePath() {
        return csvFilePath;
    }

    public void setCsvFilePath(String csvFilePath) {
        this.csvFilePath = csvFilePath;
    }

    public String getFenwickTreeFilePath() {
        return fenwickTreeFilePath;
    }

    public void setFenwickTreeFilePath(String fenwickTreeFilePath) {
        this.fenwickTreeFilePath = fenwickTreeFilePath;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public int getColumns() {
        return columns;
    }

    public void setColumns(int columns) {
        this.columns = columns;
    }

    public int[] getColumnOrdering() {
        return columnOrdering;
    }

    public void setColumnOrdering(int[] columnOrdering) {
        this.columnOrdering = columnOrdering;
    }

    public HashMap<Integer, Integer> getDistinctValuesByColumn() {
        return distinctValuesByColumn;
    }

    public void setDistinctValuesByColumn(HashMap<Integer, Integer> distinctValuesByColumn) {
        this.distinctValuesByColumn = distinctValuesByColumn;
    }

    public HashMap<Integer, ArrayList<Integer>> getColumnLimitIndexing() {
        return columnLimitIndexing;
    }

    public void setColumnLimitIndexing(HashMap<Integer, ArrayList<Integer>> columnLimitIndexing) {
        this.columnLimitIndexing = columnLimitIndexing;
    }

    public FenwickTreeFile getCsvIndexing() {
        return csvIndexing;
    }

    public void setCsvIndexing(FenwickTreeFile csvIndexing) {
        this.csvIndexing = csvIndexing;
    }

    /* This method overrides the compressCSV method provided
     * by the Compression interface
     */
    @Override
    public String compressCSV(String csvFilePath, boolean containsHeader, boolean quoted) {


        try {

            // first we get the data necessary to begin sorting the file
            long s, e;

            String header = null;

            if (containsHeader || quoted) {
                FilePreProcessor fpp = new FilePreProcessor(csvFilePath);
                header = fpp.removeHeaders(containsHeader);
                if (quoted) {
                    fpp.escapeCommas();
                }
            }

            s = System.currentTimeMillis();

            IntegerArrayAndMapWrapper iamw = Analytics.analyzeDistinctValuesByColumn(csvFilePath);

            e = System.currentTimeMillis();
            System.out.println("Analytics took " + (e - s) + "ms.");


            if (iamw != null) {

                rows = iamw.getRows();
                columns = iamw.getColumns();
                columnOrdering = iamw.getOrder();
                System.out.println("rows = " + rows + " columns = " + columns);


                s = System.currentTimeMillis();


                MyExternalSort.externalSort(csvFilePath, rows, columns, columnOrdering);
                e = System.currentTimeMillis();
                System.out.println("Sorting took " + (e - s) + "ms.");


                IndexingUnit indexingUnit = new IndexingUnit(OS_Properties.TEMP_DIR_PATH + "test_output.dat", rows, columns);
                s = System.currentTimeMillis();

                FenwickTreeFile fwt = indexingUnit.getIndexedCommas();
                e = System.currentTimeMillis();
                System.out.println("Indexing and building Fenwick tree took " + (e - s) + "ms.");

                s = System.currentTimeMillis();
                HashMap<Integer, ExternalList> indexedColumns = indexingUnit.indexColumns(OS_Properties.TEMP_DIR_PATH
                        + OS_Properties.FILE_SEPARATOR + "test_output.dat", iamw.getOrder(), fwt);

                e = System.currentTimeMillis();
                System.out.println("Second indexing and building Fenwick tree took " + (e - s) + "ms.");

                System.out.println("FWT SIZE = " + fwt.N);

                String outPath = csvFilePath.replaceAll(".csv", ".csvcomp");


                String toSearchFor = null;
                StringBuilder sb = new StringBuilder(Byte.MAX_VALUE);
                ArrayList<Integer> indices;
                StringBuilder line = new StringBuilder(Byte.MAX_VALUE * (columns + 1));


                File file = new File(outPath);
                file.createNewFile();
                PrintWriter pw = new PrintWriter(new FileWriter(file));
                ExternalList list = null;

                /* Write the header to the compressed file if the original file contains one */
                if (header != null) {
                    pw.println(header);
                }

                /* Attach a RandomAccessFile utility to the sorted output */
                this.csvRaf = new RandomAccessFile(OS_Properties.TEMP_DIR_PATH + "test_output.dat", "rw");


                s = System.currentTimeMillis();

                try {

                    //the null list has already "finished"
                    int sortedColumnStart = 0, sortedColumnEnd, firstIndex, lastIndex, count;
                    int[] next = new int[indexedColumns.size()];
                    int[] pos = new int[indexedColumns.size()];
                    SimpleBitMap finished = new SimpleBitMap(next.length);

                    // Initialize the next index in the Binary Indexed Tree which is to be read from
                    for (int i = 0; i < next.length; i++) {
                        if (indexedColumns.get(i) != null) {
                            next[i] = indexedColumns.get(i).get(pos[i]);
                        } else {
                            next[i] = i;
                        }
                    }


                    // Get the infromation about the size of single-sorted columnar intervals
                    int[] listSize = new int[next.length];
                    for (int i = 0; i < next.length; i++) {
                        if (indexedColumns.get(i) != null) {
                            listSize[i] = indexedColumns.get(i).size();
                        } else {
                            listSize[i] = -1;
                        }
                    }


                    while (finished.getCardinality() < finished.getSize()) {

                        for (int j = 0; j < next.length; j++) {
                            if (next[j] >= fwt.N) {
                                finished.set(j);
                            }
                        }


                        for (int i = 0; i < next.length; i++) {

                            if (!finished.get(i)) {
                                if (indexedColumns.get(i) != null) {

                                    list = indexedColumns.get(i);

                                    if (pos[i] < listSize[i] - 1) {
                                        if (next[i] < list.get(pos[i] + 1)) {
                                            sortedColumnStart = list.get(pos[i]);
                                        } else {
                                            pos[i]++;
                                            next[i] = list.get(pos[i]);
                                            sortedColumnStart = next[i];
                                        }
                                        if (pos[i] == listSize[i] - 1) {
                                            int x = next[i];
                                            while ((x + columns) < fwt.N) {   //go to the bottom of the column
                                                x += columns;
                                            }
                                            sortedColumnEnd = x;


                                        } else {
                                            sortedColumnEnd = list.get(pos[i] + 1) - columns;
                                        }
                                    } else if (pos[i] == listSize[i] - 1) {

                                        sortedColumnStart = list.get(pos[i]);

                                        int x = next[i];
                                        while ((x + columns) < fwt.N) {   //go to the bottom of the column
                                            x += columns;
                                        }
                                        sortedColumnEnd = x;

                                    } else {
                                        sortedColumnEnd = i + (rows - 1) * columns;
                                    }
                                } else {
                                    sortedColumnStart = i;
                                    sortedColumnEnd = i + (rows - 1) * columns;
                                }

                                byte n;
                                csvRaf.seek(fwt.read(next[i]));

                                n = (byte) csvRaf.read();

                                while ((char) n != ',' && !OS_Properties.LINE_SEPARATOR.contains(String.valueOf((char) n)) && n != -1) {
                                    sb.append((char) n);
                                    n = (byte) csvRaf.read();
                                }

                                toSearchFor = sb.toString();
                                sb.setLength(0);                                //reset the string builder

                                /* The following method invocations are used to
                                 * find the first and last occurrence of the
                                 * toSearcFor cell in the CSV.
                                 * Implemented using Binary Search
                                 */
                                firstIndex = Utilities.binarySearchFirst(toSearchFor, csvRaf, fwt, i, sortedColumnStart, sortedColumnEnd, rows, columns);
                                lastIndex = Utilities.binarySearchLast(toSearchFor, csvRaf, fwt, i, sortedColumnStart, sortedColumnEnd, rows, columns, firstIndex);


                                count = (lastIndex - firstIndex) / columns + 1;
                                line.append(toSearchFor).append(' ').append(count);

                                if (i == next.length - 1) {
                                    pw.println(line.toString());
                                    pw.flush();                                 //flush the print writer
                                    line.setLength(0);                          //reset the string builder
                                } else {
                                    line.append(',');
                                }

                                int prev = next[i];
                                next[i] = lastIndex + columns;

                            } else {

                                if (i < next.length - 1) {
                                    line.append(',');
                                } else {
                                    line.append(OS_Properties.LINE_SEPARATOR);
                                    if (line.toString().length() > 100000) {
                                        pw.print(line.toString());
                                        line.setLength(0);                      //reset the string builder
                                        pw.flush();                             //flush the print writer
                                    }
                                }
                            }
                        }
                    }
                } finally {
                    fwt.disposeTree();
                    csvRaf.close();
                    fwt.fc.close();
                    fwt.raf.close();
                    pw.close();
                }

                e = System.currentTimeMillis();
                System.out.println("Compression took " + (e - s) + "ms.");

                //return the header to the original file if it contains one
                if (containsHeader) {
                    FilePreProcessor f = new FilePreProcessor(csvFilePath);
                    f.returnHeaderAndCommasToOriginalFile(header, quoted);
                }

                return outPath;
            }


        } catch (FileNotFoundException ex) {
            System.err.printf("File %s not found.\nCompression cannot proceed.\nSystem will now exit.\n", csvFilePath);
            System.out.println(ex.getMessage());
            System.exit(1);
        } catch (IOException ex) {
            System.err.printf("Unexpected I/O exception occurred at this point.\nCompression cannot proceed.\nSystem will now exit.\n");
            System.exit(1);
        } catch (Exception ex) {
        }

        return null;

    }

    /* This method overrides the decompressCSV method provided by the
     * Compression interface.
     */
    @Override
    public void decompressCSV(String compressedCsvFilePath, boolean containsHeader, boolean quoted) {

        int index, rowsN, columnsN, currentPositionInFile;
        long bufStart, bufSize, fileSize, byteCounter = 0;
        byte n;

        FileChannel fileChannel = null;
        MappedByteBuffer IO = null;
        FenwickTreeFile fwt = null;
        PrintWriter pw = null;
        Queue<DecompressionWrapper> queues[];

        try {

            String header = null;

            if (containsHeader) {
                FilePreProcessor f = new FilePreProcessor(compressedCsvFilePath);
                header = f.removeHeaders(containsHeader);
            }

            rowsN = Analytics.rows(compressedCsvFilePath);
            columnsN = Analytics.columns(compressedCsvFilePath);

            fileChannel = new RandomAccessFile(compressedCsvFilePath, "rw").getChannel();
            fileSize = Files.size(Paths.get(compressedCsvFilePath));
            bufStart = 0;
            bufSize = fileSize < 100 * 1024 * 1024 ? fileSize : 100 * 1024 * 1024;

            IO = fileChannel.map(FileChannel.MapMode.READ_ONLY, bufStart, bufSize);

            queues = new LinkedList[columnsN];
            for (int i = 0; i < queues.length; i++) {
                queues[i] = new LinkedList<>();
            }

            IO.rewind();

            StringBuilder sb = new StringBuilder(256);
            index = 0;
            currentPositionInFile = 0;

            while (IO.hasRemaining()) {

                n = IO.get();

                if ((char) n == ',') {

                    currentPositionInFile += sb.length();
                    queues[index].add(new DecompressionWrapper(sb.toString()));
                    sb.setLength(0);
                    index = (index + 1) % columnsN;

                } else if ((char) n == OS_Properties.LINE_SEPARATOR.charAt(OS_Properties.OS_LS_INDEX)) {

                    currentPositionInFile += sb.length();
                    queues[index].add(new DecompressionWrapper(sb.toString()));
                    sb.setLength(0);
                    index = (index + 1) % columnsN;
                    break;

                } else if (!OS_Properties.LINE_SEPARATOR.contains(String.valueOf((char) n))) {
                    sb.append((char) n);
                }
            }

            StringBuilder sb2 = new StringBuilder(50 * 1024);
            DecompressionWrapper reference;
            String decompressedOutputFile = compressedCsvFilePath.replaceAll(".csvcomp", "_d.csv");
            File a = new File(decompressedOutputFile);
            a.createNewFile();
            pw = new PrintWriter(new FileWriter(a));

            fwt = new IndexingUnit(compressedCsvFilePath, rowsN, columnsN).getIndexedCommas();

            int[] lastIndex = new int[queues.length];
            for (int i = 0; i < lastIndex.length; i++) {
                lastIndex[i] = i;
            }

            SimpleBitMap bitmap = new SimpleBitMap(columnsN);
            while (bitmap.getCardinality() < bitmap.getSize()) {

                int min = Integer.MAX_VALUE, minPos = -1;

                for (int i = 0; i < queues.length; i++) {
                    if (!queues[i].isEmpty()) {
                        int count = queues[i].peek().count;
                        if (count < min) {
                            min = count;
                            minPos = i;
                        }
                    }
                }

                if (minPos == -1) {
                    break;
                }

                reference = queues[minPos].peek();

                while (reference.count > 0) {

                    for (int i = 0; i < queues.length; i++) {
                        if (!queues[i].isEmpty()) {

                            sb2.append(queues[i].peek().getText());
                            queues[i].peek().count--;

                            if (i >= 0 && i < queues.length - 1) {
                                sb2.append(',');
                            } else if (i == queues.length - 1) {
                                pw.println(sb2.toString());
                                sb2.setLength(0);
                            } else {
                                // do nothing
                            }
                        }
                    }

                }

                // remove the written content from the min-queue
                for (int i = 0; i < queues.length; i++) {
                    if (!queues[i].isEmpty()) {
                        DecompressionWrapper d = queues[i].peek();
                        if (d.count == 0) {
                            queues[i].poll();
                        }
                    }
                }

                for (int i = 0; i < queues.length; i++) {

                    if (queues[i].isEmpty()) {

                        int newFwtIndex = lastIndex[i] + columnsN;
                        int newIndexInFile;
                        if (newFwtIndex < fwt.N) {

                            newIndexInFile = (int) fwt.read(newFwtIndex);
                            lastIndex[i] = newFwtIndex;

                            if (newIndexInFile < bufStart || newIndexInFile + 128 > bufStart + bufSize) {
                                bufStart = newIndexInFile;
                                bufSize = (fileSize - bufStart) < 100 * 1024 * 1024 ? (fileSize - bufStart) : 100 * 1024 * 1024;
                                IO = fileChannel.map(MapMode.READ_ONLY, bufStart, bufSize);
                                IO.rewind();
                            }

                            sb.setLength(0);                                    //reset the string builder

                            while (IO.hasRemaining()) {
                                n = IO.get(newIndexInFile - (int) bufStart);
                                if ((char) n == ',' || (char) n == OS_Properties.LINE_SEPARATOR.charAt(OS_Properties.OS_LS_INDEX)) {
                                    break;
                                } else if (!OS_Properties.LINE_SEPARATOR.contains(String.valueOf((char) n))) {
                                    sb.append((char) n);
                                }
                                newIndexInFile++;
                            }

                            String aux = sb.toString();

                            if (!aux.isEmpty() && !aux.equals(OS_Properties.LINE_SEPARATOR) && !aux.equals(",")) {

                                currentPositionInFile += aux.length();
                                try {
                                    queues[i].add(new DecompressionWrapper(aux));
                                } catch (ArrayIndexOutOfBoundsException e) {
                                    //do nothing, it won't be created nor added to the queue
                                    currentPositionInFile -= aux.length();
                                } catch (NumberFormatException ee) {
                                }
                            }
                            sb.setLength(0);                                    //reset the string builder

                        } else {
                            bitmap.set(i);                                      //mark the queue as "finished"
                        }
                    }
                }
            }


            /* Write the header if it exists and then convert the escaped commas back to their inital format */
            if (containsHeader) {
                FilePreProcessor f = new FilePreProcessor(decompressedOutputFile);
                f.returnHeaderAndCommasToOriginalFile(header, quoted);
            }


        } catch (FileNotFoundException ex) {
            Logger.getLogger(CompressionUnit.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(CompressionUnit.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(CompressionUnit.class.getName()).log(Level.SEVERE, null, ex);
        } finally {

            if (pw != null) {
                pw.close();
            }
            try {
                if (fwt != null) {
                    fwt.raf.close();
                    fwt.fc.close();
                }

                if (fileChannel != null) {
                    fileChannel.close();
                }
            } catch (IOException e) {
            }
        }
    }
}
