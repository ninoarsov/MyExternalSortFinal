
/* This class provides a memory-efficient implementation of 
 * the Binary Indexed Tree data structure.
 */
package myexternalsort;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 *
 * @author Nino
 */

public class FenwickTreeFile {

    String path;
    FileChannel fc;
    MappedByteBuffer IO;
    long ioStart;
    long ioEnd;
    int N;  //number of commas
    static byte[] num = new byte[4];
    
    public RandomAccessFile raf;
    public long bufStart, bufSize, size;

    
    FenwickTreeFile(String path, int N) throws FileNotFoundException, IOException {
       
        this.path = path;
        fc = new RandomAccessFile(path, "rw").getChannel();
        this.N = N;
        int c = 0, len;
        while (c < 4 * N) {
            IO = fc.map(FileChannel.MapMode.READ_WRITE, c, len = (4 * N - c) < 100 * 1024 * 1024 ? (4 * N - c) : 100 * 1024 * 1024);
            byte[] src = new byte[len];
            IO.put(src);
            c += len;
        }

        IO = fc.map(FileChannel.MapMode.READ_WRITE, 0, 4 * N);
        ioStart = 0;
        ioEnd = ioStart + 4 * N;

    }

    public void add(int i, int value) throws FileNotFoundException, IOException {
        
        try {

            int len, bufInd;
            for (; i < N; i += (i + 1) & -(i + 1)) {

                int currPos = 4 * i;

                if (currPos >= ioStart && currPos + 3 <= ioEnd) {
                    bufInd = currPos - (int) ioStart;
                } else {
                    IO = fc.map(FileChannel.MapMode.READ_WRITE, currPos, len = (4 * N - currPos) < 100 * 1024 * 1024 ? (4 * N - currPos) : 100 * 1024 * 1024);
                    ioStart = currPos;
                    ioEnd = ioStart + len;
                    bufInd = 0;

                }

                int newVal = IO.getInt(bufInd) + value;
                IO.putInt(bufInd, newVal);
            }
        } catch (Exception e) {}
    }
       
    
    
    /* This method prepares the tree for reading, initializing and attaching
     * a random access file utility 
     */
    public void initRandomAccessFile() throws FileNotFoundException, IOException, Exception {
        
        final WeakReference<MappedByteBuffer> bufferWeakRef = new WeakReference<MappedByteBuffer>(IO);
        final long startTime = System.currentTimeMillis();
        while (null != bufferWeakRef.get()) {
            if (System.currentTimeMillis() - startTime > 10) {
                break;
            }
            System.gc();
            Thread.yield();
        }
      
        fc.close();
        raf = new RandomAccessFile(this.path, "rw");
        size = Files.size(Paths.get(this.path));
        bufStart = 0;
        bufSize = size;
        IO = raf.getChannel().map(MapMode.READ_ONLY, bufStart, bufSize);
    }
    
    
    /* Disposes the tree, closing the residing file streams attached to 
     * the particular file containing the tree.
     */
    public void disposeTree() throws IOException, Exception{
        
        try{
        } finally{
            if(raf != null){                
                raf.getChannel().close();
                raf.close();                    
            }
        }
        
    }

    public long read(int i) throws IOException {
    
        long sum = 0;
        int index;
        for (; i >= 0; i -= (i + 1) & -(i + 1)) {
            index = 4 * i;
            if(index < bufStart || index > bufStart + bufSize){
                bufStart = (index - 100*1024*1024) >= 0 ? (index - 100*1024*1024) : 0;
                bufSize = index < 100*1024*1024 ? index : 100*1024*1024+1;
                IO = raf.getChannel().map(MapMode.READ_ONLY, bufStart, bufSize);
                index = 0;
            }
            sum += IO.getInt(index);
        }
        return sum;
    
    }
}
