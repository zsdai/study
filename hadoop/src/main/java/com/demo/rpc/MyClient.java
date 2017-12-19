package com.demo.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {
	public static void main(String[] args) throws IOException {
		final InetSocketAddress inetSocketAddress = new InetSocketAddress("127.0.0.1", MyServer.PORT);
		final Bizable proxy = (Bizable) RPC.getProxy(Bizable.class, MyServer.VERSION, inetSocketAddress, new Configuration());
		final String ret = proxy.hello("吴超");
		System.out.println(ret);
		
		RPC.stopProxy(proxy);
	}
}
