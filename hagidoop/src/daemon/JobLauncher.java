package daemon;
import java.rmi.Naming;
import java.rmi.RemoteException;
import io.KVFileRW;
import io.TxtFileRW;
import config.Project;
import interfaces.MapReduce;

public class JobLauncher {
	static int nbMachines=2;
	static Worker[] listeWorkers = new Worker[nbMachines];
	static String[] urlWorker = {"//localhost:8000/test","//localhost:8001/test", "//localhost:8002/test", "//localhost:8003/test", "//localhost:8004/test"};
	
	public static void startJob (MapReduce mr, int format, String fname) {

		hdfs.HdfsClient.HdfsWrite(0, fname);
		try{
			for (int i = 0 ; i < nbMachines ; i++) {
				listeWorkers[i]=(Worker) Naming.lookup(urlWorker[i]);
			}
			//MyMapReduce mr = new MyMapReduce();
			for (int i = 0 ; i < 7; i++) {  //nbFRAGMEEEEENTS;
				try {
					TxtFileRW reader = new TxtFileRW(listeWorkers[i%2].getPath()+"//data//filesample-"+i+".txt");
					KVFileRW writer = new KVFileRW(listeWorkers[i%2].getPath()+"//data//filesampleres-"+i+".txt");
					listeWorkers[i%nbMachines].runMap(mr, reader, writer);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
			//java.lang.Thread.sleep(30);
//			JobLauncher job = new JobLauncher();
//			System.out.println("Lancement du Job");
//			job.startJob(mr, 0, "\\home\\wissal\\Bureau\\pdr\\Projet-Donnees-Reparties\\hagidoop\\src\\");
//			System.out.println("Fin du lancement du Job");
			//System.out.println("Lecture COMMENCÉÉÉÉ");
			hdfs.HdfsClient.HdfsRead(Project.PATH+"//data//filesampleFinal.txt");
			//System.out.println("Lecture terminée");

			KVFileRW reader = new KVFileRW(Project.PATH+"//data//filesampleFinal.txt");
			KVFileRW writer = new KVFileRW(Project.PATH+"//data//resultat.txt");
			reader.open("read");
			writer.open("write");
			
			// appliquer reduce sur le résultat
			// reader : format kv ; writer : format kv
			//System.out.println("Début du reduce");
			
			mr.reduce(reader, writer);
			//System.out.println("Fin du reduce");
			
			reader.close();
			writer.close();
		} catch (Exception exc)  {
			exc.printStackTrace();

		}
	}
}
