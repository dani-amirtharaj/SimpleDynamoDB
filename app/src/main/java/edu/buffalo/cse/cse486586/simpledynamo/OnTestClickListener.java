package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

/* Listener for when button "Test" is clicked on. */
public class OnTestClickListener implements OnClickListener {

	private static final String TAG = OnTestClickListener.class.getName();
	private static final int TEST_CNT = 50;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	private final TextView mTextView;
	private final ContentResolver mContentResolver;
	private final Uri mUri;
	private final ContentValues[] mContentValues;

	public OnTestClickListener(TextView _tv, ContentResolver _cr) {
		mTextView = _tv;
		mContentResolver = _cr;
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		mContentValues = initTestValues();
	}

	/* Builds URI for the chosen content provider. */
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	/* Initializes content values used for testing. */
	private ContentValues[] initTestValues() {
		ContentValues[] cv = new ContentValues[TEST_CNT];
		for (int i = 0; i < TEST_CNT; i++) {
			cv[i] = new ContentValues();
			cv[i].put(KEY_FIELD, "key" + i);
			cv[i].put(VALUE_FIELD, "val" + i);
		}
		return cv;
	}

	/* Handler for click events on the "Test" button. */
	@Override
	public void onClick(View v) {
		new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
	}

	private class Task extends AsyncTask<Void, String, Void> {

		@Override
		protected Void doInBackground(Void... params) {
			if (testInsert()) {
				publishProgress("Insert success\n");
			} else {
				publishProgress("Insert fail\n");
				return null;
			}

			if (testQuery()) {
				publishProgress("Query success\n");
			} else {
				publishProgress("Query fail\n");
			}
			
			return null;
		}
		
		protected void onProgressUpdate(String...strings) {
			mTextView.append(strings[0]);
			return;
		}

		/* Testing insert into the Content Provider. */
		private boolean testInsert() {
			try {
				for (int i = 0; i < TEST_CNT; i++) {
					mContentResolver.insert(mUri, mContentValues[i]);
				}
			} catch (Exception e) {
				Log.e(TAG, e.toString());
				return false;
			}
			return true;
		}

        /* Testing query from the Content Provider. */
		private boolean testQuery() {
			try {
				for (int i = 0; i < TEST_CNT; i++) {
					String key = (String) mContentValues[i].get(KEY_FIELD);
					String val = (String) mContentValues[i].get(VALUE_FIELD);

					Cursor resultCursor = mContentResolver.query(mUri, null,
							key, null, null);
					if (resultCursor == null) {
						Log.e(TAG, "Result null");
						throw new Exception();
					}

					int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
					int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
					if (keyIndex == -1 || valueIndex == -1) {
						Log.e(TAG, "Wrong columns");
						resultCursor.close();
						throw new Exception();
					}

					resultCursor.moveToFirst();

					if (!(resultCursor.isFirst() && resultCursor.isLast())) {
						Log.e(TAG, "Queried : "+key);
						Log.e(TAG, "Wrong number of rows");
						resultCursor.close();
						throw new Exception();
					}

					String returnKey = resultCursor.getString(keyIndex);
					String returnValue = resultCursor.getString(valueIndex);
					if (!(returnKey.equals(key) && returnValue.equals(val))) {
						Log.e(TAG, "(key, value) pairs don't match\n");
						resultCursor.close();
						throw new Exception();
					}

					resultCursor.close();
				}
			} catch (Exception e) {
				return false;
			}
			return true;
		}
	}
}
