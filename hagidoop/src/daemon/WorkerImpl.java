package daemon;

import java.rmi.RemoteException;

import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl implements Worker{

	@Override
	public void runMap(Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException {
		reader.open("read");
		m.map(reader, writer);
		reader.close();

	}

}
