package io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import interfaces.FileReaderWriter;
import interfaces.KV;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class TxtFileRW implements FileReaderWriter{
	private String mode;
	private String fname; // Nom du fichier.    
    private BufferedReader reader; // Utilisé pour lire à partir du fichier.
    private BufferedWriter writer; // Utilisé pour écrire dans le fichier.
    private long index = 0; // Index utilisé pour le parcours du fichier.
    private KV kv;
    
    // Constructeur
    public TxtFileRW(String fname) {
        this.fname = fname;
    }
    
	@Override
	public KV read() {
		try {
			kv.v = reader.readLine();
			kv.k = String.valueOf(index++);
			if (kv.v != null) {
				index += kv.v.length();
				return kv;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void write(KV record) {
		try {
			writer.write(record.v, 0, record.v.length());
			writer.newLine();
			index += record.v.length();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

	@Override
	public void open(String mode) {
        try {
        	this.mode = mode;
        	this.kv = new KV();
            if (mode.equals("read")) {
                this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(fname)));
            } else if (mode.equals("write")) {
                this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fname)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
		
	}

	@Override
	public void close() {
		try {
	        if (mode.equals("read")) {
	        	reader.close();
	        } else if (mode.equals("write")) {
	        	writer.close();
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}

	@Override
	public long getIndex() {
		return index;
	}

	@Override
	public String getFname() {
		return fname;
	}

	@Override
	public void setFname(String fname) {
		this.fname = fname;
		
	}
	

}