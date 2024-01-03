package hdfs;
import java.io.File;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

import interfaces.*;
import io.*;
import io.TxtFileRW;
import config.Project;

public class HdfsClient {
	 private static int nbServers = Project.listServeur.length;
	 private static String listMachines[] = Project.listMachines;
	 private static int listPorts[] = Project.listServeur;
	 private static int nbrFragments = Project.nbrFragments;
	 public static int fragmentsize = Project.fragmentsize;
	 private static KV kv = new KV("khadija","akkar");
	 private static final int bufferSize = 2000000;

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public HdfsClient() {
		
	}
	public static void HdfsDelete(String fname) {
		try{
        	int j;  // Nombre du serveur      	        	
            for (int i = 0; i < nbrFragments; i++) {
            	j = i % nbServers;
                Socket socket = new Socket (listMachines[j], listPorts[j]);
                String[] inter = fname.split("\\.");
                String nom = inter[0];
                String extension = inter[1];
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject("DELETE" + ":" + nom + "_" + Integer.toString(i) + "." + extension);  // pour apres dans hdfsServer on fait le split sur : et avoir la commande
                oos.flush(); // la bonne habitude
                oos.close();
                socket.close();
            }        	            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
	}
	
	public static void HdfsWrite(int fmt, String fname) {
		try {            
            String fragment = "";
            int index;
            KV buffer = new KV();
        	File file = new File(fname);
        	long taille = file.length();
        	int nbfragments = (int) (taille/fragmentsize);
            if (taille%fragmentsize != 0) { nbfragments ++;}                    	
        	// le cas d'un lineformat fichier:
        	
            if (fmt == FileReaderWriter.FMT_TXT){
                TxtFileRW file2 = new TxtFileRW(fname);
                file2.open("read");
                for (int i=0; i < nbfragments; i++){
                	index = 0;
                    buffer = kv ;
                    fragment = "";
                    while (index < fragmentsize){
                        buffer = file2.read();
                        if (buffer == null){break;}
                        fragment = fragment + buffer.v + "\n";
                        index = (int) (file2.getIndex() - i * fragmentsize);
                    }
                    int t = i%nbServers;
                    Socket socket = new Socket (listMachines[t], listPorts[t]);
                    String[] inter = fname.split("\\.");
                    String nom = inter[0];
                    String extension = inter[1];
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject("WRITE" + ":" + nom + "_" + Integer.toString(i) + "." + extension + ";" + fragment);
                    oos.flush(); 
                    oos.close();
                    socket.close();
                }
                file2.close();
            } else if (fmt == FileReaderWriter.FMT_KV){
	            KVFileRW file2 = new KVFileRW(fname);
	            file2.open("read");
	            for (int i=0; i < nbfragments ; i++){
	                index = 0;
	                buffer =  kv;
	                while (index < fragmentsize){
	                    buffer = file2.read();
	                    if (buffer == null){break;}
	                    fragment = fragment + buffer.v + "\n";
	                    index = (int) (file2.getIndex()-i*fragmentsize);
	                }
	                int t = i%nbServers;
	                Socket socket = new Socket (listMachines[t], listPorts[t]);
	                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    String[] inter = fname.split("\\.");
                    String nom = inter[0];
                    String extension = inter[1];
	                oos.writeObject("WRITE" + ":" + nom + "_" + Integer.toString(i) + "." + extension + ";" + fragment);
	                oos.flush();
	                oos.close();
	                socket.close();
	            }
	            file2.close(); }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
	}

	public static void HdfsRead(String fname) {
		String[] inter = fname.split("\\.");
        String nom = inter[0];
        String extension = inter[1];
        File file = new File(fname);
        try {
        	int j;
            FileWriter fw = new FileWriter(file);
            for (int i = 0; i < nbrFragments; i++) {
                j = i % nbServers;
                Socket socket = new Socket (listMachines[j], listPorts[j]);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject("READ" + ":" + nom +"_"+ Integer.toString(i) + "-result" + "." + extension);
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                String fragment = (String) ois.readObject();
                fw.write(fragment,0,fragment.length());
                ois.close();
                oos.close();
                socket.close();
            }
            fw.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
	}

	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande.
		try {
            if (args.length<3) {usage(); return;}
            switch (args[0]) {
              case "read": HdfsRead(args[2]); break;
              case "delete": HdfsDelete(args[2]); break;
              case "write": 
                int fmt;
                if (args.length < 3) {usage(); return;}
                if (args[1].equals("txt")) fmt = FileReaderWriter.FMT_TXT;
                else if(args[1].equals("kv")) fmt = FileReaderWriter.FMT_KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2]);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}


