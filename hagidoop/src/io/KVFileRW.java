package io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import interfaces.FileReaderWriter;
import interfaces.KV;

public class KVFileRW implements FileReaderWriter{
	
	private String fname; // Nom du fichier.
    private String mode; // Mode du fichier (FMT_TXT ou FMT_KV).
    private BufferedReader reader; // Utilisé pour lire à partir du fichier.
    private BufferedWriter writer; // Utilisé pour écrire dans le fichier.
    private long index; // Index utilisé pour le parcours du fichier.
    
    // Constructeur
    public KVFileRW() {
        this.fname = null;
        this.mode = null;
        this.reader = null;
        this.writer = null;
        this.index = 0;
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
	public void open(String mode) {
		this.mode = mode;
        try {
            if (mode.equals("read")) {
                this.reader = new BufferedReader(new FileReader(this.fname));
            } else if (mode.equals("write")) {
                this.writer = new BufferedWriter(new FileWriter(this.fname));
            } else if (mode.equals("delete")) {
            	File fileToDelete = new File(this.fname);
                if (fileToDelete.delete()) {
                    System.out.println("Fichier supprimé avec succès.");
                } else {
                    System.err.println("Échec de la suppression du fichier.");
                }
            } else {
                System.err.println("Format non pris en charge.");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
		
	}

	@Override
	public void close() {
		try {
            if (mode.equals("read")) {
                if (reader != null) {
                    reader.close();
                }
            } else if (mode.equals("write")) {
                if (writer != null) {
                    writer.close();
                }
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
