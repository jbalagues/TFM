package com.uoc.tfm.carga;
import java.net.Authenticator;
import java.net.PasswordAuthentication;

public class ResourceAuthenticator extends Authenticator {  
    private static String username = "";
    private static String password = "";

    protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication (ResourceAuthenticator.username, 
                ResourceAuthenticator.password.toCharArray());
    }

    public static void setPasswordAuthentication(String username, String password) {
        ResourceAuthenticator.username = username;
        ResourceAuthenticator.password = password;
    }
}