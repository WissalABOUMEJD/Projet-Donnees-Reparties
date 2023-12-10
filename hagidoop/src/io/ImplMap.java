package io;

import java.util.HashMap;

import interfaces.KV;
import interfaces.Reader;
import interfaces.Writer;

/*public void map(Reader reader, Writer writer) {

	HashMap<String,Integer> hm = new HashMap<String,Integer>();
	KV kv;
	while ((kv = reader.read()) != null) {
		String tokens[] = kv.v.split(" ");
		for (String tok : tokens) {
			if (hm.containsKey(tok)) hm.put(tok, hm.get(tok)+1);
			else hm.put(tok, 1);
		}
	}
	for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
}*/