package com.uoc.tfm.carga.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.log4j.Logger;

import com.uoc.tfm.carga.CargaTMB;
import com.uoc.tfm.carga.ResourceAuthenticator;

public class FileUtils {
	final static Logger logger = Logger.getLogger(FileUtils.class);
	
	public static void copyInputStream(InputStream in, OutputStream out) throws IOException {
		byte[] buffer = new byte[1024];
		int len;
		while ((len = in.read(buffer)) >= 0) {
			out.write(buffer, 0, len);
		}
		in.close();
		out.close();
	}

	public static void downloadFile(String urlFile, String user, String pass, String path, String outFile ) {
		try {
			long startTime = System.currentTimeMillis();

			if(user != null && pass != null){
				// Install Authenticator
				ResourceAuthenticator.setPasswordAuthentication(user, pass);
				Authenticator.setDefault(new ResourceAuthenticator());
			}
			URL url = new URL(urlFile);

			url.openConnection();
			InputStream reader = url.openStream();

			FileOutputStream writer = new FileOutputStream(path + outFile);
			byte[] buffer = new byte[102400];
			int totalBytesRead = 0;
			int bytesRead = 0;

			logger.debug("Reading ZIP file 20KB blocks at a time.\n");

			while ((bytesRead = reader.read(buffer)) > 0) {
				writer.write(buffer, 0, bytesRead);
				buffer = new byte[102400];
				totalBytesRead += bytesRead;
			}

			long endTime = System.currentTimeMillis();

			logger.debug("Done. " + new Integer(totalBytesRead).toString() + " bytes read ("
					+ new Long(endTime - startTime).toString() + " millseconds).\n");
			writer.close();
			reader.close();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static List<String> unZip(String path, String outFile) {
		List<String> listFiles = new ArrayList<String>();

		try {

			ZipFile zipFile = new ZipFile(path + outFile);

			Enumeration zipEntries = zipFile.entries();

			File OUTFILEFOLD = new File(path);
			if (!OUTFILEFOLD.exists()) {
				OUTFILEFOLD.mkdir();
			}
			String OUTDIR = path;
			while (zipEntries.hasMoreElements()) {
				ZipEntry zipEntry = (ZipEntry) zipEntries.nextElement();
				if (zipEntry.isDirectory()) {
					logger.debug("      Extracting directory: " + OUTDIR + zipEntry.getName());
					new File(OUTDIR + zipEntry.getName()).mkdir();
					continue;
				}
				logger.debug("       Extracting file: " + OUTDIR + zipEntry.getName());
				copyInputStream(zipFile.getInputStream(zipEntry),
						new BufferedOutputStream(new FileOutputStream(OUTDIR + zipEntry.getName())));

				listFiles.add(zipEntry.getName());
			}

			zipFile.close();
		} catch (IOException ioe) {
			System.err.println("Unhandled exception:");
			ioe.printStackTrace();
		}
		return listFiles;
	}
	public static void downloadURLFile(String path, String outFile, String urlString) {
	 	BufferedInputStream in = null;
	    FileOutputStream fout = null;
	    try {
	        in = new BufferedInputStream(new URL(urlString).openStream());
	        fout = new FileOutputStream(path+outFile);

	        final byte data[] = new byte[1024];
	        int count;
	        while ((count = in.read(data, 0, 1024)) != -1) {
	            fout.write(data, 0, count);
	        }
	    }catch (Exception e){
	    	logger.error(e);
	    } finally {
	        if (in != null) {
	            try {
					in.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	        if (fout != null) {
	            try {
					fout.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	    }
	}
}
