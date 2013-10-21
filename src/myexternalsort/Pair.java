
/*This class implements a simple helper utility which stores a whole row 
 * from a dataset, separated by the default CSV delimiter and also keeps the
 * position of the cell to be compared during external sorting.
 */
package myexternalsort;

/**
 *
 * @author Nino
 */
public class Pair implements Comparable<Pair> {

    public int pos;
    public String[] cols;

    Pair(String data, int pos) {
        this.pos = pos;
        cols = data.split(",");
    }

    @Override
    public int compareTo(Pair arg0) {

        int i = 0, compareIndex;
        while (true) {
            if (i < MyExternalSort.order.length) {
                if (cols[MyExternalSort.order[i]].equals(arg0.cols[MyExternalSort.order[i]])) {
                    i++;
                } else {
                    compareIndex = MyExternalSort.order[i];
                    break;
                }
            } else {
                return 0;
            }
        }

        if (cols[compareIndex].compareTo(arg0.cols[compareIndex]) > 0) {
            return 1;
        } else if (cols[compareIndex].compareTo(arg0.cols[compareIndex]) < 0) {
            return -1;
        } else {
            return 0;
        }

    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.length; i++) {
            sb = sb.append(cols[i]);
            if (i < cols.length - 1) {
                sb = sb.append(",");
            }
        }
        sb.append(OS_Properties.LINE_SEPARATOR);
        return sb.toString();

    }
}
