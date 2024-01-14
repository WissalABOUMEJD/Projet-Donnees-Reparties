package hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.io.*;
import java .net.*;

public class HdfsServer{
	private static int numPorts[]={3158, 3292, 3692, 3434, 3300, 3000};
	private static int nbServeurs = 2;
	
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
            System.out.print(message);
        	ServerSocket serverSocket = new ServerSocket(intValue);
            while(true) {
            	
            	System.out.println("Server is listening on port" + intValue);
            	Socket clientSocket = serverSocket.accept();
            	System.out.println("Client connected.");
            	ObjectInputStream objectIS = new ObjectInputStream(clientSocket.getInputStream());
                String msg = (String) objectIS.readObject();
            	//BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
            	i++;
//            	if (i>nbServeurs) {
//            		i=0;
//            	}
	            String commande;
	            //commande = input.readLine();
	            //System.out.println("Server received: " + msg);
	            String[] mots = msg.split("@");
	            String[] mots2 = msg.split("_");
	            System.out.println(mots[0]);
	            int j=0;
	            switch (mots[0]) {
		            case "read":
	                	File rFile = new File("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\filesample-" + mots2[1] +".txt");
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
	                    File wFile = new File("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\filesample-" + mots2[1] +".txt");
	                    FileWriter fWriter = new FileWriter(wFile);
	                    BufferedWriter writr = new BufferedWriter(fWriter);
	                    writr.write(mots[2], 0, mots[2].length());
	                    writr.close();
	                    fWriter.close();
	                    break;
	                case "delete":
	                	File file = new File("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\filesample-" + mots2[1] +".txt");
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
