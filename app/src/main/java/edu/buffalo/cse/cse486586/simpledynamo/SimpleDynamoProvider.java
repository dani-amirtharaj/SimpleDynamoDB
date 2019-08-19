package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

/* Content Provider to support DynamoDB like storage across multiple Android devices. */
public class SimpleDynamoProvider extends ContentProvider {

	private static String myPort;
	private static final int SERVER_PORT = 10000;
	private static final String[] REMOTE_PORTS = {"5554", "5556", "5558", "5560", "5562"};
	private static final int REPLICATION = 3;
	private List<Socket> sockets;
	private static Queue<String> failQueue;
	private static Queue<String> failCache;
	private static String failedNode;

	private AppDatabase dbHelper;
	private SQLiteDatabase rdb;
	private SQLiteDatabase wdb;

	List<String> ring = new ArrayList<>();
	HashMap<String, String> unhash = new HashMap<>();

	String myPortHash;
	Uri mUri;
	String sharedMsg;

	Lock lock = new ReentrantLock();

	/* Initialize URI for this Content Provider. */
	public SimpleDynamoProvider() {
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	}

	/* Builds URI for the chosen content provider. */
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	/* Deletes selected key across entire DynamoDB (multiple Android devices). */
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		int deleted = 0;

		/* Deletes all data stored in this Android device alone and replications in other devices. */
		if (selection.equals("@")) {
			Cursor query = rdb.query(
					Messages.FeedEntry.TABLE_NAME,   // The table to query
					new String[] {Messages.FeedEntry.COLUMN_NAME_KEY, Messages.FeedEntry.COLUMN_NAME_VALUE},             // The array of columns to return (pass null to get all)
					null,              	  // The columns for the WHERE clause
					null,          		  // The values for the WHERE clause
					null,                    // don't group the rows
					null,                     // don't filter by row groups
					null			         // The sort order
			);
			if (query != null) {
				/* Identify devices with replications and send messages to delete replications */
				try {
					/* Identify devices with key. */
					int index = Collections.binarySearch(ring, myPortHash);
					index = index < 0 ? -1 * index - 1 : index;
					index = index >= ring.size() ? 0 : index;

					/* Get keys to be deleted and send list across devices identified. */
					int keyIndex = query.getColumnIndex(Messages.FeedEntry.COLUMN_NAME_KEY);
					int failCount = 0;
					if (query.moveToFirst()) {
						while (!query.isAfterLast()) {
							for (int i=0; i<REPLICATION; i++) {
								new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, unhash.get(ring.get(index)) + ":delete:" + query.getString(keyIndex)).get();
                                if (sharedMsg != null && !sharedMsg.equals("empty")) {
                                    deleted += Integer.parseInt(sharedMsg);
                                } else if (sharedMsg == null) {
									failCount++;
								}
								index++;
								index = index >= ring.size() ? 0 : index;
								query.moveToNext();
							}
							deleted /= REPLICATION-failCount;
						}
					}
				} catch (Exception e) {
					Log.e(TAG, e.toString());
				}
			}
		  /*Deletes all data across entire DynamoDB (all Android devices). */
		} else if (selection.equals("*")) {
			for (String port : REMOTE_PORTS) {
				try {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port + ":delete:" + "@").get();
					if (sharedMsg != null && !sharedMsg.equals("empty")) {
						deleted += Integer.parseInt(sharedMsg);
					}
				} catch (Exception e) {
					Log.e(TAG, e.toString());
				}
			}
		  /* Identifies device with selected key and deletes the selected key from DynamoDB. */
		} else {
			String selectionHash = null;
			try {
				selectionHash = genHash(selection);
			} catch (Exception e) {
					Log.e(TAG, e.toString());
				}
				/* Identify device with selected key. */
				int index = Collections.binarySearch(ring, selectionHash);
				index = index < 0 ? -1 * index - 1 : index;
				index = index >= ring.size() ? 0 : index;

				/* Sends key to be deleted across device identified as well as devices with replications. */
				for (int i=0; i<REPLICATION; i++) {
					try {
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, unhash.get(ring.get(index)) + ":delete:" + selection).get();
						if (sharedMsg != null && !sharedMsg.equals("empty")) {
							deleted += Integer.parseInt(sharedMsg);
						}
					} catch (Exception e) {
							Log.e(TAG, e.toString());
						}
					index++;
					index = index >= ring.size() ? 0 : index;
				}
				deleted /= REPLICATION;
		}
		return deleted;
	}

	/* Deletes selected key from self (this Android device). */
	private int deleteFromSelf(String selection) {
		String selectionVal;
		String[] selectionArgsVal;
		selectionVal = Messages.FeedEntry.COLUMN_NAME_KEY + "=?";
		selectionArgsVal = new String[]{selection};
		int deleted = rdb.delete(
				Messages.FeedEntry.TABLE_NAME,   // The table to query
				selectionVal,              		  // The columns for the WHERE clause
				selectionArgsVal         		  // The values for the WHERE clause
		);
		return deleted;
	}

	/* Retrieves messages lost during device failure. */
	public void getLostMessages() throws Exception {
		for (int i=0; i<REMOTE_PORTS.length ;i++) {
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REMOTE_PORTS[i] + ":retrieve:empty").get();
		}
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	/* Inserts key and value across entire DynamoDB (multiple Android devices). */
	@Override
	public Uri insert(Uri uri, ContentValues values) {
		long tmstmp = System.currentTimeMillis();
		String selectionHash = null;
		try {
			selectionHash = genHash(values.getAsString(Messages.FeedEntry.COLUMN_NAME_KEY));
		} catch (Exception e) {
				Log.e(TAG, e.toString());
		}
		/* Identifies device where key is to be inserted. */
		int index = Collections.binarySearch(ring, selectionHash);
		index = index < 0 ? -1 * index - 1 : index;
		index = index >= ring.size() ? 0 : index;

		/* Sends keys to be inserted across devices identified (to hold replications). */
		for (int i=0; i<REPLICATION ;i++) {
			try {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, unhash.get(ring.get(index)) + ":insert:" + values.getAsString(Messages.FeedEntry.COLUMN_NAME_KEY) + "~" + values.getAsString(Messages.FeedEntry.COLUMN_NAME_VALUE)+"~"+tmstmp).get();
			} catch (Exception e) {
					Log.e(TAG, e.toString());
			}
			index++;
			index = index >= ring.size() ? 0 : index;
		}
		return uri;
	}

	/* Inserts key and value into self (this Android device). */
	private void insertIntoSelf(ContentValues values) {
		Cursor query = (Cursor) rdb.query(
				Messages.FeedEntry.TABLE_NAME,   // The table to query
				null,             		  // The array of columns to return (pass null to get all)
				Messages.FeedEntry.COLUMN_NAME_KEY + "=?",                         // The columns for the WHERE clause
				new String[]{values.getAsString(Messages.FeedEntry.COLUMN_NAME_KEY)},      // The values for the WHERE clause
				null,                    // don't group the rows
				null,                     // don't filter by row groups
				null                     // The sort order
		);
		if (query.getCount() == 0) {
			wdb.insert(Messages.FeedEntry.TABLE_NAME, null, values);
		} else {
			int tmsIndex = query.getColumnIndex(Messages.FeedEntry.COLUMN_NAME_TMSTMP);
			query.moveToFirst();
			if (query.getLong(tmsIndex) < values.getAsLong(Messages.FeedEntry.COLUMN_NAME_TMSTMP)) {
				wdb.update(Messages.FeedEntry.TABLE_NAME, values, Messages.FeedEntry.COLUMN_NAME_KEY + "=?",
						new String[]{values.getAsString(Messages.FeedEntry.COLUMN_NAME_KEY)});
			}
		}
	}

	/* Called during the create lifecycle of this Content Provider to initialize various objects. */
	@Override
	public boolean onCreate() {
		/* Get port used for this Android device. */
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr)));

		/* Initialize the Database and other helper objects. */
		dbHelper = new AppDatabase(getContext());
		rdb = dbHelper.getReadableDatabase();
		wdb = dbHelper.getWritableDatabase();
		failQueue = new LinkedList<>();
		failCache = new LinkedList<>();

		/* Set up network structure of the Distributed Database on each Android device.  */
		try {
			for (String port : REMOTE_PORTS) {
				String portHash = genHash(port);
				if (myPort.equals(port)) {
					myPortHash = portHash;
				}
				int index = Collections.binarySearch(ring, portHash);
				index = index < 0 ? -1 * index - 1 : index;
				ring.add(index, portHash);
				unhash.put(portHash, port);
			}
		} catch (Exception e) {
			Log.e(TAG, e.toString());
		}

		/* Initialize and start a server to receive requests. */
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			getLostMessages();
		} catch (Exception e) {
			Log.e(TAG, e.toString());
		}

		return true;
	}

	/* Queries selected key across entire DynamoDB (multiple Android devices). */
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		Cursor query = null;
		/* Queries and returns all data stored in this Android device alone and replications in other devices. */
        if (selection.equals("@")) {
            query = rdb.query(
                    Messages.FeedEntry.TABLE_NAME, // The table to query
                    new String[] {Messages.FeedEntry.COLUMN_NAME_KEY,
							Messages.FeedEntry.COLUMN_NAME_VALUE},             // The array of columns to return (pass null to get all)
                    null,              	// The columns for the WHERE clause
                    null,          	   	// The values for the WHERE clause
                    null,                 // don't group the rows
                    null,                  // don't filter by row groups
                    null				   // The sort order
            );
          /*Queries and returns all data across entire DynamoDB (all Android devices). */
        } else if (selection.equals("*")) {
			MatrixCursor retCursor = new MatrixCursor(new String[]{Messages.FeedEntry.COLUMN_NAME_KEY, Messages.FeedEntry.COLUMN_NAME_VALUE});
			for (String port : REMOTE_PORTS) {
				try {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port + ":query:" + "@").get();
				if (sharedMsg != null && !sharedMsg.equals("empty")) {
					for (String keyVals : sharedMsg.split(";")) {
						String[] keyVal = keyVals.split("~");
						retCursor.addRow(new String[]{keyVal[0], keyVal[1]});
					}
				} } catch (Exception e) {
					Log.e(TAG, e.toString());
				}
			}
			query = retCursor;
		  /* Queries and returns value for key by identifying device with key and retrieving value for key. */
		} else {
			String selectionHash;
			try {
				selectionHash = genHash(selection);

				/* Identifies device that should be queried based on key. */
				int index = Collections.binarySearch(ring, selectionHash);
				index = index < 0 ? -1 * index - 1 : index;
				index = index >= ring.size() ? 0 : index;
				long maxTmstmp = 0;

				/* Queries devices that hold key (along with replications), to retrieve the latest value. */
				for (int i = 0; i < REPLICATION; i++) {
					try {
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, unhash.get(ring.get(index)) + ":query:" + selection).get();
						if (sharedMsg != null && !sharedMsg.equals("empty")) {
							MatrixCursor retCursor = new MatrixCursor(new String[]{Messages.FeedEntry.COLUMN_NAME_KEY, Messages.FeedEntry.COLUMN_NAME_VALUE});
							for (String keyvals : sharedMsg.split(";")) {
								String[] keyVal = keyvals.split("~");
								retCursor.addRow(new String[]{keyVal[0], keyVal[1]});
								if (maxTmstmp < Long.parseLong(keyVal[2])) {
									query = retCursor;
								}
							}
						}
					} catch (Exception e) {
						Log.e(TAG, e.toString());
					}
					index++;
					index = index >= ring.size() ? 0 : index;
				}

			} catch (Exception e) {
				Log.e(TAG, "Query:"+e.toString());
			}
		}

		return query;
	}

	/* Queries and returns value from self (this Android device). */
	public Cursor querySelf(String selection) {
		String selectionVal;
		String[] selectionArgsVal;
		Cursor query = null;
		/* Returns all key value pairs in this Android device. */
		if (selection.equals("@")) {
			query = rdb.query(
					Messages.FeedEntry.TABLE_NAME,   // The table to query
					null,             		  // The array of columns to return (pass null to get all)
					null,              	  // The columns for the WHERE clause
					null,          		  // The values for the WHERE clause
					null,                    // don't group the rows
					null,                     // don't filter by row groups
					null					  // The sort order
			);
		  /* Returns selected key value pair in this Android device. */
		} else {
			selectionVal = Messages.FeedEntry.COLUMN_NAME_KEY + "=?";
			Log.e("Selected", selection);
			selectionArgsVal = new String[]{selection};
			query = rdb.query(
					Messages.FeedEntry.TABLE_NAME,   // The table to query
					null,             		  // The array of columns to return (pass null to get all)
					selectionVal,              		  // The columns for the WHERE clause
					selectionArgsVal,          		  // The values for the WHERE clause
					null,                  	  // don't group the rows
					null,                     // don't filter by row groups
					null// The sort order
			);
		}
		return query;
	}

	/* Not used, updates are performed with insert itself. */
	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		return 0;
	}

	/* Generates SHA-1 hashes which will be used to evenly distribute key space across all devices. */
	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	/* Server to handle messages and requests from other devices. */
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... servSocket) {
			ServerSocket serverSocket = servSocket[0];
			while (true) {
				try {
					Socket clientSocket = serverSocket.accept();
					new Thread(() -> {
						try (
								BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
								PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
						) {
							String received;
							while ((received = in.readLine()) != null) {

								/* Get message parameters. */
								String[] params = received.split(":");
								String fromPort = params[0];
								String type = params[2];
								String message = params[3];

								/* Call appropriate functions based on message type. */
								switch (type) {
									case "insert":
										insert(message);
										out.println("empty");
										break;
									case "query":
										out.println(query(message));
										break;
									case "delete":
										int deleted = deleteFromSelf(message);
										out.println(deleted);
										break;
									/* When other device calls getLostMessages and this device stores lost/failed messages (replications). */
									case "retrieve":
										int index = Arrays.binarySearch(REMOTE_PORTS, fromPort);
										synchronized (lock) {
											sockets.remove(index);
											Socket socket = new Socket();
											socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
													2 * Integer.parseInt(fromPort)), 1000);
											socket.setSoTimeout(0);
											sockets.add(index, socket);
											failedNode = null;
											failQueue.addAll(failCache);
											failCache.clear();
										}
										Iterator<String> itr = failQueue.iterator();

										/* Sends lost messages to device that has recently recovered from a failure. */
										while (itr.hasNext())
										{
											String queuedMessage = itr.next();
											if (queuedMessage.substring(0, 4).equals(fromPort)) {
												new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queuedMessage).get();
												itr.remove();
											}
										}
										out.println("empty");
										break;
								}
							}
							clientSocket.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}).start();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/* Parses message to a format that can be inserted and inserts into self. */
	private void insert(String message) {
		String[] keyVals = message.split(";");
		ContentValues cv = new ContentValues();
		for (String strs : keyVals) {
			String[] keyVal = strs.split("~");
			cv.put(Messages.FeedEntry.COLUMN_NAME_KEY, keyVal[0]);
			cv.put(Messages.FeedEntry.COLUMN_NAME_VALUE, keyVal[1]);
			cv.put(Messages.FeedEntry.COLUMN_NAME_TMSTMP, Long.parseLong(keyVal[2]));
		}
		insertIntoSelf(cv);
	}

	/* Parses message to a format that can be used to query and queries self and returns key value pairs. */
	private String query(String message) {
		Cursor query = querySelf(message);
		StringBuilder keyVal = null;
		if (query != null) {
			int keyIndex = query.getColumnIndex(Messages.FeedEntry.COLUMN_NAME_KEY);
			int valueIndex = query.getColumnIndex(Messages.FeedEntry.COLUMN_NAME_VALUE);
			int tmsIndex = query.getColumnIndex(Messages.FeedEntry.COLUMN_NAME_TMSTMP);
			keyVal = new StringBuilder();
			if (query.moveToFirst()) {
				while (!query.isAfterLast()) {
					keyVal.append(query.getString(keyIndex) + "~" + query.getString(valueIndex) + "~" + query.getString(tmsIndex));
					if (!query.isLast()) {
						keyVal.append(';');
					}
					query.moveToNext();
				}
			}
			return keyVal.toString();
		} else {
			return "empty";
		}
	}

	/* Client handler to send messages or requests to other devices. */
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			if (sockets == null) {
				sockets = initiateConnection();
			}
			String msgToSend = msgs[0];
			String[] params = msgToSend.split(":");
			String port = params[0];
			String type = params[1];

			int index = Arrays.binarySearch(REMOTE_PORTS, port);
			Socket socket;
			synchronized (lock) {
				/* Cache failed messages. */
				if (failedNode != null && port.equals(failedNode)) {
					failCache.add(msgToSend);
					return null;
				}
				socket = sockets.get(index);
			}
			/* Send message to device identified. */
			try {
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				out.println(myPort+":"+msgToSend);
				sharedMsg = in.readLine();
			} catch (IOException e) {
				sharedMsg = null;
			}
			if (sharedMsg == null) {
				if (type.equals("insert") || type.equals("delete")) {
					failCache.add(msgToSend);
					failedNode = port;
				}
			}
			if (type.equals("retrieve")) {
				sockets = null;
			}
			return null;
		}

		/* If no connection/sockets exist connections across all devices established. */
		private List<Socket> initiateConnection () {
			List<Socket> socketList = new ArrayList<>();
			for (String string : REMOTE_PORTS) {
				Socket socket = null;
				try {
					socket = new Socket();
					socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							2*Integer.parseInt(string)), 1000);
					socket.setSoTimeout(0);
				} catch (IOException e) {
					Log.e(TAG, e.toString());
				}
				socketList.add(socket);
			}
			return socketList;
		}
	}
}