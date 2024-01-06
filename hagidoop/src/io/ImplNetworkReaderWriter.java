package io;
import java.net.*;
import java.util.Random;

import interfaces.KV;
import interfaces.NetworkReaderWriter;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class ImplNetworkReaderWriter extends UnicastRemoteObject implements NetworkReaderWriter{
	// J'ai fait des trucs basique, aprés on ajoute ce qu'il faut pour chqe méthode
	private static final long serialVersionUID = 1L;
	private Socket socket; // Pour la création de connexion
	private BufferedReader reader; //Pour la lecture
	private BufferedWriter writer; // Pour l'écriture
	private ServerSocket serverSocket; // Pour Le serveur de connextion 
	
	protected ImplNetworkReaderWriter() throws RemoteException {
		super();
	}

	@Override
	public KV read() {
		try {
			String line = reader.readLine();
			if (line != null) {
				String[] parts = line.split("-");
				if (parts.length == 2) {
					return new KV(parts[0], parts[1]);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void write(KV record) {
		try {
			writer.write(record.toString());
			writer.newLine();
			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

	@Override
	public void openServer() {
		try {
            serverSocket = new ServerSocket(4000); // Port à choisir: J'ai fait arbitrairement 4000
            socket = serverSocket.accept(); // Pour accepter la demande de connexion 
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Pour commencer la lecture du txt du client
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); // Pour commencer l'ecriture/ preparation du outPut
            System.out.println("Server opened");    
        } catch (IOException e) {
            e.printStackTrace();
        }
		
	}

	@Override
	public void openClient() {
		// De meme avec ce que j'ai fait en haut avec openServer
		try {
            socket = new Socket("localhost", 4000); // Creer la connexion avec le serveur du port 4000
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream())); 
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            System.out.println("Client opened");
        } catch (IOException e) {
            e.printStackTrace();
        }
		
	}

	@Override
	public NetworkReaderWriter accept() {
		// J'ai fait presque la meme chose qu'on a fait en tp1/tp2 d'intergiciel 
		try {
			ImplNetworkReaderWriter connect = new ImplNetworkReaderWriter(); 
            connect.socket = serverSocket.accept(); // POur accepter la connextion 
            connect.reader = new BufferedReader(new InputStreamReader(connect.socket.getInputStream()));
            connect.writer = new BufferedWriter(new OutputStreamWriter(connect.socket.getOutputStream()));
            System.out.println("Connection accepted");
            return connect;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
	}

	@Override
	public void closeServer() {
		try {
            socket.close();
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
		
	}

	@Override
	public void closeClient() {
		try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

}
