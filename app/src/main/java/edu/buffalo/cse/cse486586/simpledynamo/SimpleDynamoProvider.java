package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = "SimpleDynamo";
	private static final String[] columnNames = {"key", "value"};
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	static boolean cursorReady = false;
	static boolean readyToSync = false;
	String[] arr = {"5554", "5556", "5558", "5560", "5562"};
	static final int SERVER_PORT = 10000;
	String[] hashednodesArray = new String[5];
	static StringBuilder sb = new StringBuilder();
	static StringBuilder sync_sb = new StringBuilder();
	HashMap<String, String> map = new HashMap<String, String>();
	static Uri mUri;
	static boolean isForwarded = false;
	static boolean insertComplete = false;
	static boolean syncComplete = false;
	static boolean replicateComplete = false;
	static boolean ack = false;
	static boolean justCreated = true;
	static String succ1 = null;
	static String succ2 = null;
	static String pred1 = null;
	static String pred2 = null;
	static String SYNC ="SYNC";
	private Object lock = new Object();


	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		String hashedKey = null;
		try {
			hashedKey = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		if (selection.equalsIgnoreCase("@") || selection.equalsIgnoreCase("*")) {
			//Delete all local files
			String[] filelist = getContext().fileList();
			for (String filename : filelist) {
				getContext().deleteFile(filename);
			}
			if (selection.equalsIgnoreCase("*")) {
			}

		} else {
			String replicatedNodes = replicas(hashedKey);
			if (replicatedNodes.contains(getMyNodeID()) || isForwarded) {
				getContext().deleteFile(selection);

			}
			if (!isForwarded) {
				String p = "DELETE".concat("-").concat(selection);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, p, replicatedNodes);
			}

		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		if(justCreated) {
			try {
				Thread.sleep(1000);
				justCreated = false;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// TODO Auto-generated method stub
		while(!syncComplete){}
		String key = values.getAsString("key");
		boolean isForward = key.contains("FORWARD")? true: false;
		if(isForward)
		{
			String[] split = key.split("-");
			key = split[0];
		}
		String hashedKey = null;
		try {
			hashedKey = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		try {

			String string = values.getAsString("value");
			String replicatedNodes = replicas(hashedKey);
			Log.e(TAG, "replicas " + replicatedNodes);
			if (replicatedNodes.contains(getMyNodeID())) {
				Log.e(TAG, "Insert at co-ordinator");
				FileOutputStream outputStream;
				synchronized (lock){
					    outputStream = getContext().openFileOutput(key.toString(), Context.MODE_PRIVATE);
						outputStream.write(string.getBytes());
						Log.v(TAG, key + "-" + string);
						outputStream.close();
				}
				if(isForward)
					return uri;
			}

			String forward = "INSERT".concat("-").concat(key).concat("-").concat(string);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward, replicatedNodes);


			while (!insertComplete) {
			}

		} catch (Exception e) {
			e.printStackTrace();
			Log.e(TAG, "Insert failed");
		}

		Log.v("Insert", values.toString());
		return uri;
	}


	public synchronized String[] node(String key) {
		Log.v(TAG, "node()");
		try {
			String[] replicatedNodes = new String[3];
			int co_ordIndex = 0;
			if (key.compareTo(hashednodesArray[0]) <= 0 || key.compareTo(hashednodesArray[4]) > 0)
				co_ordIndex = 0;
			else {
				for (int i = 1; i <= 4; i++) {
					if (key.compareTo(hashednodesArray[i - 1]) > 0 && key.compareTo(hashednodesArray[i]) <= 0)
						co_ordIndex = i;
				}
			}
			for (int j = 0; j <= 2; j++) {
				if (co_ordIndex > 4)
					co_ordIndex = co_ordIndex - 5;
				;
				replicatedNodes[j] = map.get(hashednodesArray[co_ordIndex++]);

			}

			Log.v(TAG, "returned: " + String.valueOf(replicatedNodes.length));
			return replicatedNodes;
		} catch (Exception ex) {
			Log.v(TAG, "Error creating replicated node list");
			ex.printStackTrace();
		}

		return null;
	}

	public synchronized String replicas(String key) {
		try {
			StringBuilder replicatedNodes = new StringBuilder();
			int co_ordIndex = 0;
			if (key.compareTo(hashednodesArray[0]) <= 0 || key.compareTo(hashednodesArray[4]) > 0)
				co_ordIndex = 0;
			else {
				for (int i = 1; i <= 4; i++) {
					if (key.compareTo(hashednodesArray[i - 1]) > 0 && key.compareTo(hashednodesArray[i]) <= 0)
						co_ordIndex = i;
				}
			}
			for (int j = 0; j <= 2; j++) {
				if (co_ordIndex > 4)
					co_ordIndex = co_ordIndex - 5;
				;
				replicatedNodes.append(map.get(hashednodesArray[co_ordIndex++]).concat("-"));

			}

			return replicatedNodes.toString();
		} catch (Exception ex) {
			Log.v(TAG, "Error creating replicated node list");
			ex.printStackTrace();
		}

		return null;
	}

	@Override
	public boolean onCreate() {

		syncComplete = false;
		readyToSync = false;
		ArrayList<String> hashednodesList = new ArrayList<String>();
		try {
			for (int i = 0; i < arr.length; i++) {
				String hashedNode = genHash(arr[i]);
				hashednodesList.add(hashedNode);
				map.put(hashedNode, arr[i]);
				Log.v(TAG, "key : " + hashedNode + " value: " + arr[i]);
			}
			Collections.sort(hashednodesList);
			hashednodesArray = hashednodesList.toArray(hashednodesArray);
			int myIndex = 0;
			for (int i = 0; i < hashednodesArray.length; i++) {
				if (map.get(hashednodesArray[i]).equals(getMyNodeID())) {
					myIndex = i;
					break;
				}

			}
			int succId = myIndex + 1;
			for (int j = 0; j <= 1; j++) {
				if (succId > 4)
					succId = 5 - succId;
				if (j == 0)
					succ1 = map.get(hashednodesArray[succId]);
				else
					succ2 = map.get(hashednodesArray[succId]);
				succId++;
			}

			int predId = myIndex-1;
			for (int j = 0; j <= 1; j++) {
				if (predId < 0)
					predId = 5 + predId;
				if (j == 0)
					pred1 = map.get(hashednodesArray[predId]);
				else
					pred2 = map.get(hashednodesArray[predId]);
				predId--;
			}

		} catch (NoSuchAlgorithmException ex) {

		}

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT, 50);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			//Contact successors to get data
			new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "SYNC", pred1.concat("-").concat(succ1));
			while(!readyToSync){
				//wait
			}
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}
		Log.e(TAG, "My port:" + getMyNodeID());


		if(sync_sb.length() >0)
			syncData();
		syncComplete = true;



		return true;
	}

	private void syncData(){

		if(sync_sb.charAt(sync_sb.length() -1) == ',')
			sync_sb.setLength(sync_sb.length() - 1);
		String str = sync_sb.toString();
		String pairs[] = str.split(",");
		int count = 0;
		for (String pair : pairs) {
			try {
				String[] keypair = pair.split("-");
				String hashedKey = null;
				try {
					hashedKey =  genHash(keypair[0]);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				String replicas = replicas(hashedKey);
				if(!replicas.contains(getMyNodeID()))
					continue;
				FileOutputStream outputStream;
				outputStream = getContext().openFileOutput(keypair[0].toString(), Context.MODE_PRIVATE);
				outputStream.write(keypair[1].getBytes());
				outputStream.close();
				count++;
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
		Log.v(TAG,"Synced " +String.valueOf(count) +" records");


	}

	private String getMyNodeID() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		return portStr;
	}

	public Cursor BuildCursor() {

		while (!cursorReady) {

		}
		Log.v(TAG, "Building Cursor " + sb.toString());
		if(sb.charAt(sb.length()-1) == ',')
			sb.setLength(sb.length() - 1);
		String str = sb.toString();
		String pairs[] = str.split(",");
		MatrixCursor cursor = new MatrixCursor(columnNames);
		for (String pair : pairs) {
			String[] keypair = pair.split("-");
			cursor.addRow(keypair);
		}
		Log.v(TAG, "Cursor count " + Integer.toString(cursor.getCount()));
		return cursor;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
									 String[] selectionArgs, String sortOrder) {
		String hashedKey = null;
		sb.setLength(0);
		cursorReady = false;
		boolean isForward = false;
		Log.e(TAG, "query Parameter " + selection);
		if (selection.contains("FORWARD")) {
			String[] str = selection.split("-");
			selection = str[0];
			isForward = true;
		}
		Log.e(TAG, "Updated Search " + selection);

		try {
			hashedKey = !selection.equalsIgnoreCase("@") && !selection.equalsIgnoreCase("*") ? genHash(selection) : null;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		if (selection.equalsIgnoreCase("@") || selection.equalsIgnoreCase("*")) {
			int c;
			FileInputStream inputStream;
			FileOutputStream outputStream;
			try {

				Log.e(TAG, "Search @|* query");
				MatrixCursor cursor = new MatrixCursor(columnNames);
				synchronized (lock) {
					String[] filelist = getContext().fileList();
					for (String filename : filelist) {
						StringBuilder message = new StringBuilder();
						inputStream = getContext().openFileInput(filename);

						while ((c = inputStream.read()) != -1) {
							message.append((char) c);
						}
						if (message != null) {
							String[] pair = {filename, message.toString()};
							cursor.addRow(pair);
							sb.append(pair[0].concat("-").concat(pair[1]));
							sb.append(",");
							inputStream.close();
							Log.v(TAG, "@ " + pair[0].concat("-").concat(pair[1]));
						}
					}
				}

				if (selection.equalsIgnoreCase("*") && !isForward) {
					Log.e(TAG, "Forward query");
					String p = "QUERY".concat("-@");
					new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, p, "ALL");
					//Read from String Builder
					return BuildCursor();
				}
				return cursor;
			} catch (FileNotFoundException ex) {
				Log.e(TAG, " File Not Found");
			} catch (Exception ex) {
				ex.printStackTrace();
				Log.e(TAG, "Content provider query failed");
			}
		} else {
			String replicatedNodes = replicas(hashedKey);
			Log.e(TAG, "Query  nodes" + replicatedNodes);
			boolean found = false;
			if (replicatedNodes.contains(getMyNodeID()) || isForward) {
				while(!found) {
					try {

						int c;
						StringBuilder message = new StringBuilder();
						FileInputStream inputStream;
						MatrixCursor cursor = new MatrixCursor(columnNames);
						synchronized (lock){
								inputStream = getContext().openFileInput(selection);
								while ((c = inputStream.read()) != -1) {
									message.append((char) c);
								}
								if (message != null) {
									String[] pair = {selection, message.toString()};
									cursor.addRow(pair);
									Log.v(TAG, "Found " + selection + " Value: " + message);
									inputStream.close();
								}
						}
						return cursor;
					} catch (FileNotFoundException ex) {
						Log.e(TAG, selection + " File Not Found");
						found = false;
					} catch (Exception ex) {
						found = false;
						ex.printStackTrace();
						Log.e(TAG, "Content provider query failed");
					}
				}

			} else {
				Log.e(TAG, "Forward query");
				String p = "QUERY".concat("-").concat(selection);
				new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, p, replicatedNodes);

			}
			//Read from String Builder
			return BuildCursor();

		}

		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private String queryLocal(String selection){
		StringBuilder localQueryResult =new StringBuilder();
		Log.e(TAG, "query local Parameter " + selection);

		if (selection.equalsIgnoreCase("@")) {
			int c;
			FileInputStream inputStream;
			try {

				Log.e(TAG, "Search local @ query");
				synchronized (lock) {
				String[] filelist = getContext().fileList();
				for (String filename : filelist) {
					StringBuilder message = new StringBuilder();
							inputStream = getContext().openFileInput(filename);
							while ((c = inputStream.read()) != -1) {
								message.append((char) c);
							}
							if (message != null) {
								localQueryResult.append(filename.concat("-").concat(message.toString()));
								localQueryResult.append(",");
								inputStream.close();
							}
					}
				}
				Log.v(TAG, "Local @ done ");
				return localQueryResult.toString();
			} catch (FileNotFoundException ex) {
				Log.e(TAG, " File Not Found");
			} catch (Exception ex) {
				ex.printStackTrace();
				Log.e(TAG, "Content provider query failed");
			}
		} else {
			try {
						int c;
						StringBuilder message = new StringBuilder();
						FileInputStream inputStream;
						synchronized (lock) {
								inputStream = getContext().openFileInput(selection);
								while ((c = inputStream.read()) != -1) {
									message.append((char) c);
								}
								if (message != null) {
									Log.v(TAG, "Local query found " + selection + " Value: " + message);
									localQueryResult.append(selection).append("-").append(message).append(",");
									inputStream.close();
								}
						}

				return localQueryResult.toString();

					} catch (FileNotFoundException ex) {
						Log.e(TAG, selection + " File Not Found");
					} catch (Exception ex) {
						ex.printStackTrace();
						Log.e(TAG, "Content provider query failed");
					}
		}
		return null;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket socket;
			while (true) {
				try {
					socket = serverSocket.accept();

					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					PrintWriter dos = new PrintWriter(socket.getOutputStream());
					String msg;
					Log.e(TAG, "Socket accepted");
					if ((msg = in.readLine()) != null) {
						while(!syncComplete){}
						Log.v(TAG, "ST" + msg);
						if (msg.contains("QUERY")) {
							String result = "";
							String[] split = msg.split("-");
							Log.v(TAG, "Querying " + split[1]);
				            result = queryLocal(split[1]);
							dos.println(result);
							dos.close();
						} else if (msg.contains("DELETE")) {
							isForwarded = true;
							String[] split = msg.split("-");
							Log.v(TAG, "Deleting " + split[1]);
							delete(mUri, split[1], null);
							isForwarded = false;
							dos.println("ACK");
							dos.close();

						} else if (msg.contains("INSERT"))  {
//							if (msg.contains("COORD")) {
//								ack = false;
//								publishProgress(msg);
//								while(!ack){}
//								dos.println("ACK");
//								dos.close();
//							}
								String[] split = msg.split("-");
								FileOutputStream outputStream;
								synchronized (lock) {
									outputStream = getContext().openFileOutput(split[1].toString(), Context.MODE_PRIVATE);
									Log.e(TAG, "Received forward insert");
									Log.v(TAG, "Inserting key" + split[1] + " Value: " + split[2]);
									outputStream.write(split[2].getBytes());
									outputStream.close();
								}
								if (msg.contains("COORD")) {
									ack = false;
									publishProgress(msg);
									while (!ack) {
										//wait
									}
								}
								dos.println("ACK");
								dos.close();
						}
						in.close();
					}
					socket.close();
					Log.v(TAG, "ST closed");
				} catch (IOException ex) {
					Log.v(TAG, "ST IO Exception");
					ex.printStackTrace();
				}

			}

		}

		protected void onProgressUpdate(String... p) {

			if (p[0].contains("INSERT")) {
				try {
					String[] split = p[0].split("-");
					String forward = "INSERT".concat("-").concat(split[1]).concat("-").concat(split[2]).concat("-").concat("REPLICATE");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward, succ1.concat("-").concat(succ2));
					while(!replicateComplete){
						//wait
					}
					ack = true;
				} catch (Exception e) {
					Log.e(TAG, "onProgressUpdate Insert Failed");
				}
			} else if (p[0].contains("QUERY")) {
				//I am the coordinator //Contact the two successors
				Log.v(TAG, "getting ack from succ");
				new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, p[0], succ1.concat("-").concat(succ2));
			}


			return;
		}
	}

	private class QueryClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			String msgToSend = msgs[0];
			Log.v(TAG,"QCT created");
			if (msgToSend.contains("COORD")) {
				String[] nodes = msgs[1].split("-");
				for (int i = 0; i < nodes.length; i++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(nodes[0]) * 2);
						OutputStreamWriter dos = new OutputStreamWriter(socket.getOutputStream());
						BufferedReader in =
								new BufferedReader(
										new InputStreamReader(socket.getInputStream()));
						msgToSend = msgToSend.replace("COORD", "");
						dos.write(msgToSend + "\n");

						dos.flush();
						dos.close();

						Log.e(TAG, "CT sent " + msgToSend + " to " + nodes[0]);
						String msg;
						if ((msg = in.readLine()) != null) {
							Log.e(TAG, "CT received " + msg);
						}
						in.close();
						socket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
				synchronized (this) {
					ack = true;
				}

			} else if (msgToSend.contains("QUERY") && msgs[1].contains("ALL")) {
				String[] nodes = arr;
				for (int i = 0; i < nodes.length; i++) {
					if (getMyNodeID().equals(nodes[i]))
						continue;
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(nodes[i]) * 2);
						PrintWriter dos = new PrintWriter(socket.getOutputStream());
						BufferedReader in =
								new BufferedReader(
										new InputStreamReader(socket.getInputStream()));

						dos.println(msgToSend);
						dos.flush();
						Log.e(TAG, "CT sent " + msgToSend + " to " + nodes[i]);
						String msg;
						if ((msg = in.readLine()) != null) {
							Log.e(TAG, "CT received " + msg);
							synchronized (this) {
								sb.append(msg);
							}
						}
						in.close();
						dos.close();
						socket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
				synchronized (this) {
					cursorReady = true;
				}


			} else if (msgToSend.contains("QUERY")) {
				msgs[1] = msgs[1].substring(0, msgs[1].length() - 1);
				String[] nodes = msgs[1].split("-");
				for (int i = 0; i < 3; i++) {
					try {
						if(nodes[i].equals(getMyNodeID()))
							continue;;
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(nodes[i]) * 2);
						PrintWriter dos = new PrintWriter(socket.getOutputStream());
						BufferedReader in =
								new BufferedReader(
										new InputStreamReader(socket.getInputStream()));
						if (i == 0 && !msgs[1].contains(getMyNodeID()))
							dos.println(msgToSend.concat("-").concat("COORD"));
						else
							dos.println(msgToSend);
						dos.flush();
						Log.e(TAG, "CT sent " + msgToSend + " to " + nodes[i]);
						String msg;
						if ((msg = in.readLine()) != null) {
							Log.e(TAG, "CT received " + msg);
							if(!msg.contains("NOT FOUND"))
								synchronized (this) {
									sb.append(msg);
									cursorReady = true;
									break;
								}
						}
						in.close();
						dos.close();
						socket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			}else if(msgToSend.contains("SYNC"))
			{
				String[] nodes = msgs[1].split("-");
				for (int i = 0; i < nodes.length; i++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(nodes[i]) * 2);

						Log.v(TAG,"Checking with neighbour " + nodes[i]);
						PrintWriter dos = new PrintWriter(socket.getOutputStream());
						BufferedReader in =
								new BufferedReader(
										new InputStreamReader(socket.getInputStream()));

						dos.println("QUERY-@");
						dos.flush();
						String msg;
						if ((msg = in.readLine()) != null) {
							Log.e(TAG, "CT received from predeccessor");
							synchronized (this) {
								sync_sb.append(msg);
								Log.v(TAG,"Sync received: "+ sync_sb.toString());
							}
						}
						in.close();
						dos.close();
						socket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			}
			synchronized (this) {
				readyToSync = true;
			}

			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			String msgToSend = msgs[0];
			if (msgToSend.contains("DELETE")) {
				msgs[1] = msgs[1].substring(0, msgs[1].length() - 1);
				String[] nodes = msgs[1].split("-");
				for (int i = 0; i < 3; i++) {
					if (getMyNodeID().equals(nodes[i]))
						continue;
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(nodes[i]) * 2);
						PrintWriter dos = new PrintWriter(socket.getOutputStream());
						BufferedReader in =
								new BufferedReader(
										new InputStreamReader(socket.getInputStream()));
						dos.println(msgToSend);
						dos.flush();
						Log.e(TAG, "CT sent " + msgToSend + " to " + nodes[i]);
						String msg;
						if ((msg = in.readLine()) != null) {
							Log.e(TAG, "CT received " + msg);
						}
						in.close();
						dos.close();
						socket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}

			}else if(msgToSend.contains("REPLICATE")) {
				String[] nodes = msgs[1].split("-");
				for (int i = 0; i < nodes.length; i++) {
					try {

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(nodes[i]) * 2);
						PrintWriter dos = new PrintWriter(socket.getOutputStream());
						BufferedReader in =
								new BufferedReader(
										new InputStreamReader(socket.getInputStream()));
						dos.println(msgToSend.replace("-REPLICATE", ""));
						dos.flush();
						Log.e(TAG, "CT Replicate sent " + msgToSend + " to " + nodes[i]);
						String msg;
						while ((msg = in.readLine()) != null) {
							Log.e(TAG, "CT received " + msg);
						}
						in.close();
						dos.close();
						socket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}

				synchronized (this) {
					replicateComplete = true;
				}


			} else {

				String[] nodes = msgs[1].split("-");
				boolean forwardToCoord = !msgs[1].contains(getMyNodeID()) ? true : false;
				for (int i = 0; i < nodes.length; i++) {
					try {
						if (nodes[i].equals(getMyNodeID()))
							continue;
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(nodes[i]) * 2);

						PrintWriter dos = new PrintWriter(socket.getOutputStream());
						BufferedReader in =
								new BufferedReader(
										new InputStreamReader(socket.getInputStream()));
						if (i == 0 && forwardToCoord)
							dos.println(msgToSend.concat("-").concat("COORD"));
						else
							dos.println(msgToSend);
						dos.flush();
						Log.e(TAG, "CT sent " + msgToSend + " to " + nodes[i] + " " + forwardToCoord + String.valueOf(i));
						String msg;
						while ((msg = in.readLine()) != null) {
							if (i == 0 && forwardToCoord) {
								//Received ack from coord
								Log.v(TAG, "Sent/received insert complete to coordinator");
								synchronized (this) {
									insertComplete = true;
								}
								in.close();
								dos.close();
								socket.close();
								return null;
							}
							Log.e(TAG, "CT received " + msg);
						}
						in.close();
						dos.close();
						socket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}

				synchronized (this) {
					insertComplete = true;
				}


			}
			return null;
		}


	}


}