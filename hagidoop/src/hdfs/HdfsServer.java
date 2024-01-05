package hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import interfaces.FileReaderWriter;
import interfaces.KV;
import io.TxtFileRW;
import config.Project;


public class HdfsServer extends Thread {

    private Socket clientSocket;
    private static int listServeur[] = Project.listServeur;

    public HdfsServer(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    public static void main(String[] args) {
        try {
        	for (int i:listServeur) {
	            int port = Integer.parseInt(args[0]);  //////////////////////////////////////
	            ServerSocket serverSocket = new ServerSocket(port);
	            System.out.println("Server started on port: " + args[0]);
	            while (true) {
	                Socket clientSocket = serverSocket.accept();
	                Thread t = new HdfsServer(clientSocket);
	                t.start();
	            }
        	}
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try (
            ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
            ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream())
        ) {
        	String message = (String) ois.readObject();
        	String[] tableau = message.split(":"); 
            switch (tableau[0]) {
                case "READ":
                    handleRead(ois, oos);
                    break;
                case "WRITE":
                    handleWrite(ois, oos);
                    break;
                case "DELETE":
                    handleDelete(tableau[1], oos);
                    break;
                default:
                    // Handle unknown command
                    break;
            }
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleRead(ObjectInputStream ois, ObjectOutputStream oos) throws IOException, ClassNotFoundException {
    	String message = (String) ois.readObject();
    	String[] tableau = message.split(":"); 

        // Read the content of the file
        String fileContent = readFileContent(tableau[1]);

        // Send the content back to the client
        oos.writeObject(fileContent);
    }

    private void handleWrite(ObjectInputStream ois, ObjectOutputStream oos) throws IOException, ClassNotFoundException {
        // Read the filename from the client
    	String message = (String) ois.readObject();
    	String[] tableau = message.split(":"); 

        // Read the format from the client
        FileReaderWriter fileReaderWriter = new TxtFileRW(tableau[1]);

        // Write key-value pairs to the file
        Object receivedObject;
        while ((receivedObject = ois.readObject()) instanceof KV) {
        	fileReaderWriter.write((KV) receivedObject);
        }

        // Close the format
        fileReaderWriter.close();
    }

    private void handleDelete(String filename, ObjectOutputStream oos) throws IOException, ClassNotFoundException {

        deleteFile(filename);
    }

    private String readFileContent(String fileName) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
            return content.toString();
        }
    }

    private void deleteFile(String fileName) {
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
    }
}
