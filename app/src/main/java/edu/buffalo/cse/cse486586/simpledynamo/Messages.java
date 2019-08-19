package edu.buffalo.cse.cse486586.simpledynamo;

import android.provider.BaseColumns;

/*
 * Referred the following link on how and why it is important
 * to create this contract class,
 * https://developer.android.com/training/data-storage/sqlite
 */

public final class Messages {
    // To prevent someone from accidentally instantiating the contract class,
    // make the constructor private.
    private Messages() {}
    /* Inner class that defines the table contents */
    public static class FeedEntry implements BaseColumns {
        public static final String TABLE_NAME = "Messages";
        public static final String COLUMN_NAME_KEY= "key";
        public static final String COLUMN_NAME_VALUE = "value";
        public static final String COLUMN_NAME_TMSTMP = "tmstmp";

    }
}