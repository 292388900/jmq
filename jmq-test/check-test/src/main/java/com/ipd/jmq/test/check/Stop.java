package com.ipd.jmq.test.check;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;


public class Stop {
	
	public static void main(String[] args) {
		Socket socket = null;  
        try {  
            socket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(Util.get("stopPort", "9991")));
            PrintWriter out = new PrintWriter(socket.getOutputStream());  
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));  
            out.println("stop"); 
            out.flush();
            String res = in.readLine();  
            System.out.println(res);  
            out.close();  
            in.close();  
        } catch (UnknownHostException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally {  
            if (socket != null) {  
                try {  
                    socket.close();  
                } catch (IOException e) {  
                }  
            }  
        }  
	}
	
}
