package daemon;

import java.net.MalformedURLException;
import java.nio.channels.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;

import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl extends UnicastRemoteObject implements Worker {
	static Registry registry;
	
    protected WorkerImpl() throws RemoteException {
        super();
    }


	@Override
	public void runMap(Map m, FileReaderWriter reader, FileReaderWriter writer) throws RemoteException {
		reader.open("read");
		writer.open("write");
		m.map(reader, writer);
		reader.close();
		writer.close();
	}
	
	public static void main (String args[]) {
		int i = 0;
		Scanner scanner = new Scanner(System.in);
        String message;
        message = scanner.nextLine();
        int port = Integer.parseInt(message);
		String host = "localhost";
		
		// Création du serveur de noms sur le port indiqué
		try {
			registry = LocateRegistry.createRegistry(port);
			String url = "//" + host + ":" + port + "/test";
			System.out.print("url"+url);
			WorkerImpl obj = new WorkerImpl();
			Naming.rebind(url, obj);
			System.out.print("done");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


//	@Override
//	public int testt() throws RemoteException {
//		return 333333333;
//		
//	}
}
