
/* This class implements a file pre-processing unit which the determins 
 * the pressence of a header or double-quoted text delimiters (") and modifies
 * it in order to prepare it for compression / decompression
 */
package myexternalsort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Stack;

/**
 *
 * @author Nino
 */
public class FilePreProcessor {

    private String filePath;

    public FilePreProcessor(String filePath) {
        this.filePath = filePath;
    }

    /* This method removes and returns the header of the file that is being 
     * prepared for compression.
     */
    public String removeHeaders(boolean containsHeaders) throws IOException {

        File fileToBeCompressed = new File(filePath);

        if (containsHeaders) {

            File tempFile = new File(fileToBeCompressed.getParent() + OS_Properties.FILE_SEPARATOR + "temp.csv");
            StringBuilder header = new StringBuilder();
            RandomAccessFile raf = new RandomAccessFile(fileToBeCompressed, "rw");
            FileChannel original_channel = raf.getChannel();
            FileChannel tmp_file_channel = new FileOutputStream(tempFile).getChannel();

            try {
                raf.seek(0);                                                    // set the file pointer at position 0 in the file
                header.append(raf.readLine());

                long fileToBeCompressed_size = Files.size(fileToBeCompressed.toPath());
                long offset = raf.getFilePointer();
                long chunkSize = (64 * 1024 * 1024) - (32 * 1024);                  // Windows magic number = 64MB - 32KB (best IO time performance)

                while (offset < fileToBeCompressed_size) {
                    offset += original_channel.transferTo(offset, chunkSize, tmp_file_channel);
                }

            } finally {
                if (tmp_file_channel != null) {
                    tmp_file_channel.close();
                    tmp_file_channel = null;
                }
                if (original_channel != null) {
                    original_channel.close();
                    original_channel = null;
                }
                raf.close();
            }

            // replace the original file with the new header-less file            
            Files.move(tempFile.toPath(), fileToBeCompressed.toPath(), StandardCopyOption.REPLACE_EXISTING);

            return header.toString();

        }
        return null;
    }

    /* This method replaces every occurence of a comma delimiter within a cell
     * with a rarely used character (`)
     * NOTE : Escaping commas is NOT possible when the cells are not wrapped in double quotes (" ") 
     */
    public void escapeCommas() throws FileNotFoundException, IOException {

        File fileToBeCompressed = new File(this.filePath);
        File tempFile = new File(fileToBeCompressed.getParent() + OS_Properties.FILE_SEPARATOR + "tmp.csv");
        Stack<Boolean> stack = new Stack<>();

        tempFile.createNewFile();

        long offset = 0;
        long fileSize = Files.size(fileToBeCompressed.toPath());
        int chunkSize = (64 * 1024 * 1024) - (32 * 1024);
        int capacity = ((int) fileSize < chunkSize) ? (int) fileSize : chunkSize;

        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        FileInputStream fis = new FileInputStream(fileToBeCompressed);
        FileOutputStream fos = new FileOutputStream(tempFile);
        FileChannel inChannel = fis.getChannel();

        try {
            int bytesRead = 0;
            while (offset < fileSize) {
                offset += (bytesRead = inChannel.read(buffer));

                if (buffer != null) {
                    int i = 0;

                    buffer.rewind();

                    while (i < bytesRead) {
                        if (!stack.empty()) {
                            char curr = (char) buffer.get(i);
                            if (curr == ',') {
                                buffer.put(i, (byte) '`');
                            } else if (curr == '"') {
                                stack.pop();
                            }
                            i++;
                        } else {
                            if ((char) buffer.get(i) == '"') {
                                stack.push(true);
                            }
                            i++;
                        }

                    }

                    buffer.flip();
                    fos.write(buffer.array());
                    buffer.rewind();
                }
            }

        } finally {
            inChannel.close();
            fis.close();
            fos.close();
        }

        Files.move(tempFile.toPath(), fileToBeCompressed.toPath(), StandardCopyOption.REPLACE_EXISTING);

    }

    /* This method returns the removed header to the original file after its
     * removal due to compression
     */
    public void returnHeaderAndCommasToOriginalFile(String header, boolean quoted) throws FileNotFoundException, IOException {

        File fileToBeCompressed = new File(this.filePath);
        File tempFile = new File(fileToBeCompressed.getParent() + OS_Properties.FILE_SEPARATOR + "temp.csv");
        tempFile.createNewFile();

        BufferedReader br = new BufferedReader(new FileReader(fileToBeCompressed));
        PrintWriter pw = new PrintWriter(new FileWriter(tempFile));

        try {
            pw.println(header);
            String line;
            while ((line = br.readLine()) != null) {
                pw.println(line);
            }
        } finally {
            pw.close();
            br.close();
        }
        System.out.println("RETURNING HEADER AND COMMAS PASSED....");
        /* Overwrite the existing file */

        Files.move(tempFile.toPath(), fileToBeCompressed.toPath(), StandardCopyOption.REPLACE_EXISTING);

    }
}
