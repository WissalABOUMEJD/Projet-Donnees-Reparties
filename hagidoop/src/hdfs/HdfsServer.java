package hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.io.*;
import config.Project;

public class HdfsServer{
	
	private static String readFileContents(String fileName) throws IOException {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        }
        return content.toString();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException{
        try {
        	int i = 0;
        	Scanner scanner = new Scanner(System.in);
            String message;
            message = scanner.nextLine();
            int intValue = Integer.parseInt(message);
            //System.out.print(message);
        	ServerSocket serverSocket = new ServerSocket(intValue);
            while(true) {
            	
            	System.out.println("Server is listening on port" + intValue);
            	Socket clientSocket = serverSocket.accept();
            	//System.out.println("Client connected.");
            	ObjectInputStream objectIS = new ObjectInputStream(clientSocket.getInputStream());
                String msg = (String) objectIS.readObject();
                PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
            	i++;

	            String commande;
	            String[] mots = msg.split("@");
	            String[] mots2 = msg.split("_");
	            //System.out.println(mots[0]);
	            int j=0;
	            switch (mots[0]) {
		            case "read":
	                	File rFile = new File(Project.PATH+"//data//filesampleres-" + mots2[1] +".txt");
	                	System.out.println(Project.PATH+"//data//filesampleres-" + mots2[1] +".txt");
	                    BufferedReader bufReader = new BufferedReader(new FileReader(rFile));
	                    String fragment = "";
	                    String d = bufReader.readLine();
	                    while (d != null) {
	                        fragment = fragment + d + "\n";
	                        d = bufReader.readLine();
	                    }
	                    ObjectOutputStream objectOS = new ObjectOutputStream(clientSocket.getOutputStream());
	                    objectOS.writeObject(fragment);
	                    bufReader.close();
	                    objectOS.close();
	                    break;
	                case "write":
	                    File wFile = new File(Project.PATH+"//data//filesample-" + mots2[1] +".txt");
	                    System.out.println(Project.PATH+"//data//filesampleres-" + mots2[1] +".txt");
	                    FileWriter fWriter = new FileWriter(wFile);
	                    BufferedWriter writr = new BufferedWriter(fWriter);
	                    writr.write(mots[2], 0, mots[2].length());
	                    writr.close();
	                    fWriter.close();
	                    break;
	                case "delete":
	                	File file = new File(Project.PATH+"//data//filesample-" + mots2[1] +".txt");
                        file.delete();
                        break;
	                default:
	                    break;
	            }
	            // Close the connections
	            objectIS.close();
	            output.close();
	            clientSocket.close();
	            
	        }
	        //serverSocket.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
