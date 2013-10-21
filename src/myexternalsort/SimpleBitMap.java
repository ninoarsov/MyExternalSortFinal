
/* This class implements a bitmap, as suggested by its name.
 * It is used during decompression to mark the decompressed columns as they 
 * get fully unpacked or decompressed.
 */
package myexternalsort;

/**
 *
 * @author Nino
 */

public class SimpleBitMap {

    private boolean[] bits;
    private int size;
    private int cardinality;

    public SimpleBitMap(int size) {
        this.cardinality = 0;
        this.size = size;
        this.bits = new boolean[size];
    }

    public int getCardinality() {
        return this.cardinality;
    }

    public boolean[] getBits() {
        return this.bits;
    }

    public int getSize() {
        return this.size;
    }

    public void set(int index) {
        if (!bits[index]) {
            bits[index] = true;
            cardinality++;
        }
    }

    public boolean get(int index) {
        return bits[index];
    }

    @Override
    public String toString() {
        
        StringBuilder sb = new StringBuilder(size * 5);
        for (int i = 0; i < size; i++) {
            sb.append(bits[i]).append(' ');
        }
        sb.append('\n');
        return sb.toString();
        
    }
}
