package daemon;
import hdfs.HdfsClient;
import java.rmi.Naming;
import java.rmi.RemoteException;

import interfaces.FileReaderWriter;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;
import io.KVFileRW;
import io.TxtFileRW;
import interfaces.KV;

public class JobLauncher {
	static int nbMachines=2;
	//static Worker[] listeWorkers = new Worker[nbMachines];
	//String[] urlWorker = {"//localhost:8000/test","//localhost:8001/test", "//localhost:8002/test", "//localhost:8003/test", "//localhost:8004/test"};
	
	public static void startJob (MapReduce mr, int format, String fname) {
//		int nbfragments = 3;
//		for (int i = 0 ; i < nbfragments; i++) {
//			TxtFileRW reader = new TxtFileRW(fname+"filesample-"+i+".txt");
//			KVFileRW writer = new KVFileRW(fname+"filesample-"+i+"res.txt");
//			System.out.println("****************1111");
//			try {
//				listeWorkers[i%nbMachines].runMap(mr, reader, writer);
//				System.out.println("****************");
//				writer.open("read");
//				KV kv;
//	            while ((kv = writer.read()) != null) {
//	                System.out.println("Key: " + kv.k + ", Value: " + kv.v);
//	            }
//	            System.out.println("khadiiiiiiiiija");
//			} catch (RemoteException e) {
//				e.printStackTrace();
//			}
//		}
		
	}
}
