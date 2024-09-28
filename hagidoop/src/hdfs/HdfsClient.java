package hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

import interfaces.KV;
import io.KVFileRW;
import io.TxtFileRW;

public class HdfsClient {
	private static int numPorts[]={3158, 3292};
    private static String nomMachines[]={"localhost", "localhost", "localhost", "localhost", "localhost", "localhost"};
    private static int nbServers = 2;
	private static int taille_fragment = 200;
    
	private static KV cst = new KV("hi","hello");
    public static int nbFragments(String fname) {
        try {
            String fragment = "";
            int index;
            File file = new File(fname);
            long taille = file.length();
            int nbfragments = (int) (taille / taille_fragment);

            if (taille % taille_fragment != 0) {
                nbfragments++;
            }

            return nbfragments;

        } catch (Exception ex) {
            ex.printStackTrace();
            return -1; //retourne -1 en cas d'erreur.
        }
    }
    
	public static void HdfsDelete(String hdfsFname) {
        try{
        	int j ;
        	int nbfragments = nbFragments(hdfsFname);
            for (int i = 0; i < nbfragments; i++) {
            	j = i % nbServers;
                Socket sock = new Socket (nomMachines[j], numPorts[j]);
                //System.out.println("Connected to server" + numPorts[j]);
                String[] inter = hdfsFname.split("@");
                ObjectOutputStream objectOS = new ObjectOutputStream(sock.getOutputStream());
                objectOS.writeObject("delete" + "@" + hdfsFname + "_" + Integer.toString(i) + "_"+ "." + "txt");
                objectOS.close();
                sock.close();
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	
	public static void HdfsRead(String fileName) {
		int nbfragments = 7;
        File file = new File(fileName);
        try {
        	int j;
            FileWriter fWrite = new FileWriter(file);
            //int nbfragments = 7;
            for (int i = 0; i < nbfragments; i++) {
                //System.out.println(Integer.toString(i));
                j = i % nbServers;
                //System.out.println(Integer.toString(j));
                Socket socket = new Socket (nomMachines[j], numPorts[j]);
                //System.out.println("Connected to server" + numPorts[j]);
                ObjectOutputStream objectOS = new ObjectOutputStream(socket.getOutputStream());
                objectOS.writeObject("read" + "@" + fileName + "_" + Integer.toString(i) + "_"+ "." + "txt");
                ObjectInputStream objectIS = new ObjectInputStream(socket.getInputStream());
                String fragment = (String) objectIS.readObject();
                //System.out.println("FRAAAG :" +fragment);
                fWrite.write(fragment,0,fragment.length());
                objectIS.close();
                objectOS.close();
                objectOS.flush();
                socket.close();
            }
            fWrite.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	
	public static void HdfsWrite(int fmt, String fileName) {
		
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            int bytesRead;
            int fragmentCount = 1;
            int nbFragments = nbFragments(fileName);
            KV buffer = new KV();
            
            if (fmt == 0) {
            	TxtFileRW fichier = new TxtFileRW(fileName);
                fichier.open("read");
            
            	for (int i=0; i < nbFragments; i++){
                
	            	int index = 0;	
	            	buffer = cst ;
	            	String fragment = "";
	            	while (index < taille_fragment){
	            		buffer = fichier.read();
	            		if (buffer == null){break;}
	            		fragment = fragment + buffer.v + "\n";
	            		index = (int) (fichier.getIndex()-i*taille_fragment);
	            	}
	            	int t = i%nbServers;
	            	//System.out.println(numPorts[t]);
	            	Socket socket = new Socket (nomMachines[t], numPorts[t]);
	                //System.out.println("Connected to server" + numPorts[t]);
	                //System.out.println(fragment);
	                ObjectOutputStream objectOS = new ObjectOutputStream(socket.getOutputStream());
	                objectOS.writeObject("write" + "@" + fileName + "_" + Integer.toString(i) + "_" + "txt" + "@" + fragment);
	                objectOS.flush();
                    objectOS.close();
                    socket.close();
            	}
            	fichier.close();
            } else if (fmt == 1){
                KVFileRW fichier = new KVFileRW(fileName);
                fichier.open("read");
                for (int i=0; i < nbFragments ; i++){
                    int index = 0;
                    buffer =  cst;
                    String fragment = "";
                    while (index < taille_fragment){
                        buffer = fichier.read();
                        if (buffer == null){break;}
                        fragment = fragment + buffer.v + "\n";
                        System.out.println(fragment);
                        index = (int) (fichier.getIndex()-i*taille_fragment);
                    }                    
                    int t = i%nbServers;
                    Socket socket = new Socket (nomMachines[t], numPorts[t]);
                    //System.out.println("Connected to server" + numPorts[t]);
                    //System.out.println(fragment);
                    ObjectOutputStream objectOS = new ObjectOutputStream(socket.getOutputStream());
                    objectOS.writeObject("write" + "@" + fileName + "_" + Integer.toString(i) + "." + "txt" + "@" + fragment);
                    objectOS.flush();
                    objectOS.close();
                    socket.close();
                }
                fichier.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            // Gérer l'erreur de lecture du fichier principal
        }
    }
	
	
//    public static void main(String[] args) {
//        try {
//            // Read user input and send it to the server
//            Scanner scanner = new Scanner(System.in);
//            String message;
//
//            do {
//                System.out.print("Enter message (or 'exit' to quit): ");
//                message = scanner.nextLine();
//                //output.println(message);
//                String[] mssg = message.split(" ");
//                // Receive and print the server's response
//                //String serverResponse = input.readLine();
//                //System.out.println("Server: " + serverResponse);
//
//                // If the command is "read," print the file contents
//                if (message.startsWith("read")) {
//                	HdfsRead(mssg[1]);
//                	System.out.println("REAAAAAD");
//                }
//                else if (message.startsWith("write")) {
//                	HdfsWrite(0, mssg[1]);
//                }
//                else if (message.startsWith("delete")) {
//                	System.out.println("DELEEEETE");
//                	HdfsDelete(mssg[1]);
//                }
//            } while (!message.equalsIgnoreCase("exit"));
//
//            // Close the connections
////            input.close();
////            output.close();
////            socket.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
	
}







