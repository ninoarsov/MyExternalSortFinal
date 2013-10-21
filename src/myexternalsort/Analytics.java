/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package myexternalsort;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

/**
 *
 * @author Nino
 */

public final class Analytics {

    public static final int BUFFER_SIZE = 4 * 1024;

    private static void unmap(FileChannel fc, MappedByteBuffer bb) throws Exception {
        Class<?> fcClass = fc.getClass();
        java.lang.reflect.Method unmapMethod = fcClass.getDeclaredMethod("unmap", new Class[]{java.nio.MappedByteBuffer.class});
        unmapMethod.setAccessible(true);
        unmapMethod.invoke(null, new Object[]{bb});
    }

    public static int rows(String filePath) throws FileNotFoundException, IOException, Exception {
        byte n;
        int rows = 0;
        long s, e, size, pos = 0;
        size = Files.size(Paths.get(filePath));
        s = 0;
        e = size < 100 * 1024 * 1024 ? size : 100 * 1024 * 1024;

        FileChannel fc = new RandomAccessFile(filePath, "rw").getChannel();
        MappedByteBuffer IO = null;
        try {

            IO = fc.map(FileChannel.MapMode.READ_ONLY, s, e);

            while (IO.hasRemaining()) {
                if ((char) (n = IO.get()) == OS_Properties.LINE_SEPARATOR.charAt(OS_Properties.OS_LS_INDEX)) {
                    rows++;
                }
                pos++;
                if (pos < s || pos > s + e) {
                    s = pos;
                    e = (size - s) < 100 * 1024 * 1024 ? (size - s) : 100 * 1024 * 1024;
                    IO = fc.map(FileChannel.MapMode.READ_ONLY, s, e);
                }
            }
        } finally {
            if (fc != null) {
                fc.close();
                final WeakReference<MappedByteBuffer> bufferWeakRef = new WeakReference<MappedByteBuffer>(IO);
                final long startTime = System.currentTimeMillis();
                while (null != bufferWeakRef.get()) {
                    if (System.currentTimeMillis() - startTime > 10) {
                        break;
                    }
                    System.gc();
                    Thread.yield();
                }
            }
        }
        return rows;

    }

    public static int columns(String filePath) throws FileNotFoundException, IOException, Exception {
        int columns = 1;
        FileChannel fc = null;
        MappedByteBuffer IO = null;
        long size = Files.size(Paths.get(filePath));
        try {
            fc = new RandomAccessFile(filePath, "rw").getChannel();
            IO = fc.map(FileChannel.MapMode.READ_ONLY, 0, size < 100 * 1024 * 1024 ? size : 100 * 1024 * 1024);

            IO.rewind();

            while (IO.hasRemaining()) {
                char c = (char) IO.get();
                if (c == OS_Properties.LINE_SEPARATOR.charAt(OS_Properties.OS_LS_INDEX)) {
                    break;
                } else {
                    if (c == ',') {
                        columns++;
                    }
                }
            }
        } finally {
            if (fc != null) {
                fc.close();
            }
        }

        return columns;

    }

    /* This method is used to get some useful information about the file we want to compress.
     * The output data contains the rows count, columns count and the optimal column order
     * with respect to unique single-column values.
     */
    public static IntegerArrayAndMapWrapper analyzeDistinctValuesByColumn(String filePath) throws FileNotFoundException, IOException, Exception {


        long size = Files.size(Paths.get(filePath));

        IntegerArrayAndMapWrapper wrapper = new IntegerArrayAndMapWrapper();

        String line = null;
        int columns = 1;
        long lineCount = 0;
        byte commaIndex = 0;
        boolean done = false;

        FileChannel fc = new RandomAccessFile(filePath, "rw").getChannel();

        MappedByteBuffer IO = fc.map(FileChannel.MapMode.READ_ONLY, 0, size < 400 * 1024 * 1024 ? size : 400 * 1024 * 1024);

        while (IO.hasRemaining()) {
            byte n = IO.get();
            char c = (char) n;
            if (c == OS_Properties.LINE_SEPARATOR.charAt(OS_Properties.OS_LS_INDEX)) {
                break;
            } else {
                if (c == ',') {
                    columns++;
                }
            }
        }

        IO.rewind();

        BitSet[] bitSets = new BitSet[columns];
        for (int i = 0; i < columns; i++) {
            bitSets[i] = new BitSet();
        }

        String[] cols;
        StringBuilder sb = new StringBuilder(Byte.MAX_VALUE * columns);

        long x = System.currentTimeMillis();
        try {

            char c;
            for (long i = 0; i < size; i++) {
                if (!IO.hasRemaining()) {
                    IO = fc.map(FileChannel.MapMode.READ_ONLY, i, (size - i) < 400 * 1024 * 1024 ? (size - i) : 400 * 1024 * 1024);
                    System.out.println("NEW BUFFER!");
                }

                byte n = IO.get();
                c = (char) n;

                if (!OS_Properties.LINE_SEPARATOR.contains(String.valueOf(c))) {
                    sb.append(c);
                } else if (c == OS_Properties.LINE_SEPARATOR.charAt(OS_Properties.OS_LS_INDEX)) {

                    lineCount++;
                    String l = sb.toString();
                    cols = l.split(",");


                    for (int j = 0; j < cols.length; j++) {
                        if (!cols[j].isEmpty()) {
                            int hash = Math.abs(cols[j].hashCode()) % (int) 100000;     //100000 is a raw expectation of single-column unique values
                            bitSets[j].set(hash);
                        }
                    }


                    sb.setLength(0);
                } else {
                    continue;
                }

            }

        } finally {
            fc.close();
        }


        long y = System.currentTimeMillis();

        int[] cardinalities = new int[columns];
        final HashMap<Integer, Integer> hm = new HashMap<>();
        ArrayList<Integer> l = new ArrayList<>();


        for (int i = 0; i < columns; i++) {
            cardinalities[i] = bitSets[i].cardinality();
            l.add(i);
            hm.put(i, cardinalities[i]);
        }


        /* Sort the indexed columns with respect to the number of distinct values in each column */
        Collections.sort(l, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                if (hm.get(o1) > hm.get(o2)) {
                    return 1;
                } else if (hm.get(o1) < hm.get(o2)) {
                    return -1;
                }
                return 0;
            }
        });


        /* the output of the columnar sorting above is now being assigned
         * to the cardinalities array
         */
        cardinalities = new int[columns];
        for (int i = 0; i < columns; i++) {
            cardinalities[i] = l.get(i);
        }


        // set the wrapper and return it
        wrapper.setRows((int) lineCount);
        wrapper.setColumns(columns);
        wrapper.setOrder(cardinalities);
        wrapper.setHm(hm);


        System.out.println(lineCount + "\n" + columns);

        return wrapper;
    }
}