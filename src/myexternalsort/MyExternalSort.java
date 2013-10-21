
/* This class implements an algorithm known as "External sorting", based on
 * the fast Merge Sort algorithm.  It is used for soritng large quantities of 
 * data which do not fit in a reasonable amount of main memory.
 */

package myexternalsort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 *
 * @author Nino
 */

public class MyExternalSort {

    public static int[] order;

    
    /* A simple comparator method used in columnar sorting */
    public static int compareByColumn(String o1, String o2) {
        
        String[] cols = o1.split(",");
        String[] rhsCols = o2.split(",");

        int i = 0, compareIndex;
        while (true) {
            if (i < order.length) {
                if (cols[order[i]].equals(rhsCols[order[i]])) {
                    i++;
                } else {
                    compareIndex = order[i];
                    break;
                }
            } else {
                return 0;
            }
        }

        if (cols[compareIndex].compareTo(rhsCols[compareIndex]) > 0) {
            return 1;
        } else if (cols[compareIndex].compareTo(rhsCols[compareIndex]) < 0) {
            return -1;
        } else {
            return 0;
        }

    }

    public static void externalSort(String path, int rows, int columns, int[] sortOrdering) throws IOException {

        order = sortOrdering;
        
        File file = new File(path);        
        InputStream fis = new FileInputStream(path);
        byte[] buffer = new byte[Analytics.BUFFER_SIZE];

        int M, n;
        M = 100000; //read a 100000-line chunk of the original file

        File[] temp = new File[(int) Math.ceil((double) rows / M)];
        System.out.println("Temporary files to be created: " + temp.length);

        try {
            
            List<String> block = new LinkedList<>();
            StringBuilder sb = new StringBuilder();
            int lines = 0, left = rows, fileCounter = 0;
            
            outer:
            while ((n = fis.read(buffer)) != -1) {
                for (int i = 0; i < n; i++) {
                    if ((char) buffer[i] != '\n') {
                        sb.append((char) buffer[i]);
                    } else {
                        lines++;
                        left--;
                        sb.append('\n');
                        block.add(sb.toString());
                        sb = new StringBuilder();
                        if (lines == M || left == 0) {
                            Collections.sort(block, new Comparator<String>() {
                                @Override
                                public int compare(String o1, String o2) {
                                    return compareByColumn(o1, o2);
                                }
                            });

                            temp[fileCounter] = new File(OS_Properties.TEMP_DIR_PATH + "temp" + String.valueOf(fileCounter) + ".dat");

                            OutputStream fos = new FileOutputStream(temp[fileCounter]);
                            OutputStreamWriter osw2 = new OutputStreamWriter(fos);

                            try {
                                for (String it : block) {
                                    osw2.write(it);
                                }
                            } finally {
                                osw2.close();
                                fos.close();
                            }
                            fileCounter++;
                            lines = 0;
                            block.clear();
                            if (left == 0) {
                                break outer;
                            }
                        }
                    }
                }
            }

        } finally {
            fis.close();
        }


        // N-way Merge
        FileWriter fw = new FileWriter(OS_Properties.TEMP_DIR_PATH + "test_output.dat");
        PrintWriter pw = new PrintWriter(fw);
        BufferedReader[] brs = new BufferedReader[temp.length];
        Queue<Pair> pq = new PriorityQueue<>();
        
        for (int i = 0; i < temp.length; i++) {
            brs[i] = new BufferedReader(new FileReader(temp[i]));
        }

        try {
            
            for (int i = 0; i < temp.length; i++) {
                pq.add(new Pair(brs[i].readLine(), i));
            }

            while (!pq.isEmpty()) {
                Pair min = pq.poll();
                pw.print(min);
                String next = brs[min.pos].readLine();
                if (next != null) {
                    pq.add(new Pair(next, min.pos));
                } else {
                    try{
                    } finally{
                        brs[min.pos].close();
                        temp[min.pos].delete();
                    }
                }
            }
        } finally {
            fw.close();
            pw.close();
           
            for (int i = 0; i < brs.length; i++) {
                brs[i].close();
                temp[i].delete();
            }
        }
    }
    
}
