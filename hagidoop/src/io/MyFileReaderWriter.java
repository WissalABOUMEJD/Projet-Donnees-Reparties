package io;
import java.io.*;
import interfaces.FileReaderWriter;
import interfaces.KV;
import java.util.Scanner;

public class MyFileReaderWriter implements FileReaderWriter{
	private String fname; // Nom du fichier.
    private String mode; // Mode du fichier (FMT_TXT ou FMT_KV).
    private BufferedReader reader; // Utilisé pour lire à partir du fichier.
    private BufferedWriter writer; // Utilisé pour écrire dans le fichier.
    private long index; // Index utilisé pour le parcours du fichier.
    
    // Constructeur
    public MyFileReaderWriter() {
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

	@Override
	public void write(KV record) {
		try {
			writer.write(record.toString());
			writer.newLine();
			// Pour vider le tampon et s'assurer que tout est ecrits.
			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

	@Override
	public void open(String mode) {
		this.mode = mode;
        try {
            if (FMT_TXT == Integer.parseInt(mode)) {
                this.reader = new BufferedReader(new FileReader(this.fname));
            } else if (FMT_KV == Integer.parseInt(mode)) {
                this.writer = new BufferedWriter(new FileWriter(this.fname));
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
            if (FMT_TXT == Integer.parseInt(mode)) {
                if (reader != null) {
                    reader.close();
                }
            } else if (FMT_KV == Integer.parseInt(mode)) {
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
