package com.demo.rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface Bizable extends  VersionedProtocol{
	public abstract String hello(String name);
}
class Biz implements Bizable{
	@Override
	public String hello(String name){
		System.out.println("被调用了");
		return "hello "+name;
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		System.out.println("Biz.getProtocalVersion()="+MyServer.VERSION);
		return MyServer.VERSION;
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		System.out.println(protocol+" "+clientVersion+" "+clientMethodsHash );
		return null;
	}


}