package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/*
 * Referred the following link on how to use SqlLite,
 * https://developer.android.com/training/data-storage/sqlite
 * with focus on how to specify a database schema in SqlLite with the
 * SqlLiteOpenHelper class and contract Class.
 */

public class AppDatabase extends SQLiteOpenHelper {

    private static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " + Messages.FeedEntry.TABLE_NAME + " (" +
                    Messages.FeedEntry.COLUMN_NAME_KEY + " TEXT PRIMARY KEY," +
                    Messages.FeedEntry.COLUMN_NAME_VALUE + " TEXT," +
                    Messages.FeedEntry.COLUMN_NAME_TMSTMP+" INTEGER)";

    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + Messages.FeedEntry.TABLE_NAME;

    // If you change the database schema, you must increment the database version.
    public static final int DATABASE_VERSION = 1;
    public static final String DATABASE_NAME = "Messages.db";

    public AppDatabase(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(SQL_CREATE_ENTRIES);
    }
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        // This database is only a cache for online data, so its upgrade policy is
        // to simply to discard the data and start over
        db.execSQL(SQL_DELETE_ENTRIES);
        onCreate(db);
    }
    public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        onUpgrade(db, oldVersion, newVersion);
    }
}