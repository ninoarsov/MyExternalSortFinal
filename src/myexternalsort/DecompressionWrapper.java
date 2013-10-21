/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package myexternalsort;

/**
 *
 * @author Nino
 */
public class DecompressionWrapper {

    public byte[] bytes;
    public int count;

    public DecompressionWrapper(String value) {

        StringBuilder sb = new StringBuilder();
        int i = value.length() - 1;
        while (value.charAt(i) != ' ') {
            sb.append(value.charAt(i));
            i--;
        }


        this.count = Integer.parseInt(sb.reverse().toString());
        char[] arr = value.substring(0, i).toCharArray();
        this.bytes = new byte[arr.length];
        for (int j = 0; j < arr.length; j++) {
            this.bytes[j] = (byte) arr[j];
        }

    }

    public String getText() {

        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (int i = 0; i < bytes.length; i++) {
            sb.append((char) bytes[i]);
        }
        return sb.toString();

    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            sb.append((char) bytes[i]);
        }
        sb.append(' ').append(count);
        return sb.toString();

    }
}
