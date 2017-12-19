package com.demo.rpc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;


public class MyServer {
	public static int PORT = 3242;
	public static long VERSION = 23234l;
	
	public static void main(String[] args) throws IOException {
		//final Server server = RPC.getServer(new Biz(), "127.0.0.1", PORT, new Configuration());
		Server server = new RPC.Builder(new Configuration()).setProtocol(Bizable.class) 
                .setInstance(new Biz()).setBindAddress("127.0.0.1").setPort(PORT) 
                .setNumHandlers(5).build();
		server.start();
	}
}
