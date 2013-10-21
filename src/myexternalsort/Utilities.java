
/* This class implements a modified version of Binary Search, optimized for 
 * performing binary search in a file on the disk.  It is used during
 * compression in order to boost time performance.
 */
package myexternalsort;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

/**
 *
 * @author Nino
 */

public final class Utilities {

    public static boolean[] isBinarySearchCompressionBetter(HashMap<Integer, Integer> hm, int rows) {

        /* Since linear compression of a single column with N rows takes T(N) = N steps,
         * and binary search compression takes T(N) = 2 * M * log2(N),
         * we simply check if 2 * M * log2(N) < N holds;
         * <=> M < N / (2 * log2(N))
         * NOTE: <NOT YET IMPLEMENTED>
         */

        boolean[] isItBetter = new boolean[hm.size()];

        for (Integer i : hm.keySet()) {
            int M = hm.get(i);
            isItBetter[i] = (M < (rows / (2 * (Math.log(rows) / Math.log(2)))));
        }

        return isItBetter;
    }

    public static int binarySearchFirst(String value, RandomAccessFile csvRaf, FenwickTreeFile f, int column, int columnStart, int columnEnd, int rows, int columns) throws IOException {

        /* columnStart & columnEnd are the indices of the Fenwick Tree that are
         * later being read as the limits of the column due to columnar sorting
         * in the case of a fully sorted column, they are equal to -1.
         * This method returns the index in the Fenwick Tree to be read from and
         * obtains the byte index of the first occurrence of "value" in the given column
        */
        
        
        if(columnStart == columnEnd) return columnStart;
        
        long lo, hi, mid, currentRows = (columnEnd - columnStart) / columns + 1;
        int i, j, midIndex, firstOccurrence = -1;
        String cellVal = null;
        StringBuilder sb = new StringBuilder(127);

        if (columnStart == -1 && columnEnd == -1) {
            i = column;
            j = (int) (currentRows - 1) * columns + column;
        } else {
            i = columnStart;
            j = columnEnd;
        }

        lo = f.read(i);
        hi = f.read(j);

        

        while (i <= j && currentRows > 0) {

            if (currentRows % 2 == 1) {
                midIndex = (i + j) / 2;
            } else {
                midIndex = (i + j) / 2 - columns / 2;
            }

            mid = f.read(midIndex);
            csvRaf.seek(mid);

            byte n = csvRaf.readByte();
            while (!OS_Properties.LINE_SEPARATOR.contains(String.valueOf((char)n)) && (char) n != ',' && n != -1) {
                sb.append((char) n);
                n = csvRaf.readByte();
            }

            cellVal = sb.toString();
            sb.setLength(0);                                                    //reset the string builder

            if (cellVal.equals(value)) {
                firstOccurrence = midIndex;
                j = midIndex - columns;
                hi = f.read(j);
            } else if (cellVal.compareTo(value) < 0) {
                i = midIndex + columns;
                lo = f.read(i);
            } else {
                j = midIndex - columns;
                hi = f.read(j);
            }
            currentRows = (j - i) / columns + 1;

        }

        if (firstOccurrence != -1) {
            return firstOccurrence;
        }

        return -1;
    }

    
    /* This method has an additional input parameter, the first occurence of "value".
     * It is used to read the next value in the particular column and 
     * speeed-up the search by avoiding a second binary search for the last 
     * occurrence if the next cell contains a different value (which means that
     * the particular value occurs only once in the specified interval).
     * If the value occurrs more than once, the next value in the column is 
     * guaranteed to be equal to the current value we search for due to 
     * prior columnar sorting.
     */
    public static int binarySearchLast(String value, RandomAccessFile csvRaf, FenwickTreeFile f, int column, int columnStart, int columnEnd, int rows, int columns, int firstOccurrence) throws IOException {

        /* columnStart & columnEnd are the indices of the Fenwick Tree that are 
         * later being read as the limits of the column due to columnar sorting
         * In the case of a fully sorted column, they are equal to -1.
         * This method returns the index in the Fenwick Tree to read from and 
         * obtains the byte index of the last occurrence of ",value" in the given column of the file
         */
                
        if(columnStart == columnEnd) return columnStart;
        
        // check if "value" occurrs only once
        if(firstOccurrence < columnEnd){
            csvRaf.seek(f.read(firstOccurrence + columns));
            byte n;
            StringBuilder strb = new StringBuilder();
            while((char)(n = csvRaf.readByte()) != ','){
                if(OS_Properties.LINE_SEPARATOR.contains(String.valueOf((char)n)))
                    break;
                else
                    strb.append((char)n);
            }
            if(!strb.toString().equals(value)){
                return firstOccurrence;         //firstOccurrence == lastOccurrence
            }
        }
        long lo, hi, mid, currentRows = (columnEnd - columnStart) / columns + 1;
        int i, j, midIndex, lastOccurrence = Integer.MAX_VALUE;
        String cellVal = null;
        StringBuilder sb = new StringBuilder(127);

        if (columnStart == -1 && columnEnd == -1) {
            i = column;
            j = (int) (currentRows - 1) * columns + column;
        } else {
            i = columnStart;
            j = columnEnd;
        }

        lo = f.read(i);
        hi = f.read(j);
        
        

        while (i >= columnStart && j <= columnEnd && i <= j && currentRows > 0) {

            if (currentRows % 2 == 1) {
                midIndex = (i + j) / 2;
            } else {
                midIndex = (i + j) / 2 - columns / 2;
            }

            mid = f.read(midIndex);
            csvRaf.seek(mid);

            byte n = csvRaf.readByte();
            while (!OS_Properties.LINE_SEPARATOR.contains(String.valueOf((char)n)) && (char) n != ',' && n != -1) {
                sb.append((char) n);
                n = csvRaf.readByte();
            }

            cellVal = sb.toString();
            sb.setLength(0);                                                    //reset the string builder

            if (cellVal.equals(value)) {
                lastOccurrence = midIndex;
                i = midIndex + columns;
            } else if (cellVal.compareTo(value) < 0) {
                i = midIndex + columns;
            } else {
                j = midIndex - columns;
            }
            currentRows = (j - i) / columns + 1;

        }

        if (lastOccurrence != Integer.MAX_VALUE) {
            return lastOccurrence;
        }

        return -1;
    }
}
