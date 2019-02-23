package com.homework3.ylc.weekly_report;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//        System.out.println( "Hello World!" );
    	QueryHive qh = new QueryHive("Weekly Report");
    	String start = "2015-01-01";
    	String end = "2015-01-07";
    	qh.make_report(start, end);
    }
}
