
/* Wrapper class for several integer fields as well as an integer array
 * and a hash-map.  This is the actual output from the initial analysis
 * of the soon to-be compressed file.
 */
package myexternalsort;

import java.util.HashMap;

/**
 *
 * @author Nino
 */
public class IntegerArrayAndMapWrapper {
    
    private int[] order;
    private int rows;
    private int columns;
    private HashMap<Integer, Integer> hm;


    public IntegerArrayAndMapWrapper() {
    }

    public int[] getOrder() {
        return order;
    }

    public void setOrder(int[] order) {
        this.order = order;
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

    public HashMap<Integer, Integer> getHm() {
        return hm;
    }

    public void setHm(HashMap<Integer, Integer> hm) {
        this.hm = hm;
    }
 
}
