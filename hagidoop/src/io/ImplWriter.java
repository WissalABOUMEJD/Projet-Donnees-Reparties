package io;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;

import interfaces.KV;
import interfaces.Writer;

public class ImplWriter implements Writer, Serializable{
	
	private transient BufferedWriter buffer;
	
	public ImplWriter (BufferedWriter buffer) {
		this.buffer = buffer;
	}
	

	@Override
	public void write(KV record) {
		try {
			buffer.write(record.toString());
			buffer.newLine();
			// Pour vider le tampon et s'assurer que tout est ecrits.
			buffer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} 
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
