package daemon;

import java.rmi.Naming;
import java.rmi.RemoteException;
import application.MyMapReduce;
import hdfs.HdfsClient;
import io.KVFileRW;
import interfaces.KV;
import io.TxtFileRW;

public class Client {
	public static void main(String[] args) {
		int nbMachines=2;
		Worker[] listeWorkers = new Worker[nbMachines];
		String[] urlWorker = {"//localhost:8000/test","//localhost:8001/test", "//localhost:8002/test", "//localhost:8003/test", "//localhost:8004/test"};
		try{
			for (int i = 0 ; i < nbMachines ; i++) {
				listeWorkers[i]=(Worker) Naming.lookup(urlWorker[i]);
				System.out.println("LLOOOLL");
			}
			MyMapReduce mr = new MyMapReduce();
			for (int i = 0 ; i < 7; i++) {  //nbFRAGMEEEEENTS;
				//File wfile = new File("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\filesampleres-"+i+".txt");
				TxtFileRW reader = new TxtFileRW("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\filesample-"+i+".txt");
				KVFileRW writer = new KVFileRW("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\filesampleres-"+i+".txt");
				System.out.println("****************1111");
				System.out.println(listeWorkers);
				System.out.println("READER"+ reader);
				System.out.println("WRITER"+ writer);
				try {
					listeWorkers[i%nbMachines].runMap(mr, reader, writer);
					System.out.println("****************");
					writer.open("read");
					KV kv;
		            while ((kv = writer.read()) != null) {
		                System.out.println("Key: " + kv.k + ", Value: " + kv.v);
		            }
		            System.out.println("khadiiiiiiiiija");
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
//			JobLauncher job = new JobLauncher();
//			System.out.println("Lancement du Job");
//			job.startJob(mr, 0, "\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\");
//			System.out.println("Fin du lancement du Job");
			//HdfsClient.HdfsRead("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\mmmm.txt");
			System.out.println("Lecture terminée");
			//File kfile = new File("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\resTemp.txt");
			KVFileRW reader = new KVFileRW("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\resTemp.txt");
			reader.open("write");
			for (int i = 0 ; i < 7; i++) {
				KVFileRW reader1 = new KVFileRW("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\filesampleres-"+i+".txt");
				reader1.open("read");
				KV kv;
	            while ((kv = reader1.read()) != null) {
	                reader.write(kv);
	            }
			}
			reader.close();
			//KVFileRW reader = new KVFileRW("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\filesampleres-2.txt");
			//File mfile = new File("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\lol.txt");
			KVFileRW writer = new KVFileRW("\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\lol.txt");
			
			reader.open("read");
			writer.open("write");
			
			// appliquer reduce sur le résultat
			// reader : format kv ; writer : format kv
			System.out.println("Début du reduce");
			
			mr.reduce(reader, writer);
			System.out.println("Fin du reduce");
			
			reader.close();
			writer.close();
		} catch (Exception exc)  {
			exc.printStackTrace();

		}
	}
}
