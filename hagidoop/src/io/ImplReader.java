package io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import interfaces.KV;
import interfaces.Reader;

public class ImplReader implements Reader{
	
	// Buffer du lectude du fichier
	private BufferedReader buffer;
	 
	public ImplReader(String pathFile) {
		try {
			this.buffer = new BufferedReader(new FileReader(pathFile));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
	}
	
	@Override
	public KV read() {
		try {
			String line = buffer.readLine();
			if (line != null) {
				// ça dépent de la forme de fichier!!!!!!!!!!!!!!!!
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
	
	// Pour gérer la fermeture de notre buffer.
	public void close() {
        try {
            buffer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
